"""Real-git split-phase fault-injection state machine (§3.B / §D8 / §D11).

`test_d8_connect_splitphase.py` covers *named* crash points one at a time
(crash after Phase A, after Phase B, fence bump, trailer false-match, …), and
`spec/omd_connect.tla` model-checks an **abstract** version where Phase B is a
coin-flip (OK/ERR) and recovery keys off an in-model `mergeResult` rather than
git. Neither exercises a **randomized sequence** of faults against an *actual*
git repository — which is where split-phase reconciliation really earns its
keep (a merge that landed in git but never got recorded in the DB; a token left
dangling by a crash; a rolled-back task that must remain re-connectable).

This harness drives a real on-disk git repo + persistent SQLite coordinator
through random interleavings of:

  * develop a task in its own worktree/branch (disjoint write-set per task),
  * connect it cleanly (all three phases) → MERGED,
  * connect with Phase B forced to fail → rollback to DONE (retryable),
  * **crash after Phase A** (CONNECTING, token held, nothing merged),
  * **crash after Phase B** (the merge LANDED in git, DB still CONNECTING),
  * **restart** the coordinator (close DB, re-open same file+repo) so `_recover()`
    reconciles every CONNECTING task with git truth via trailer-probe,
  * idempotent re-connect of an already-MERGED task.

After every step it re-checks the load-bearing invariants: the repo-wide merge
token is never double-held and never leaks once nothing is in flight; a task is
MERGED **iff** its merge commit is actually in the integration branch (no
phantom merge, no double merge), with its write lease released and merge_sha
recorded; the integration worktree is never left dirty (no dangling MERGE_HEAD);
and the leader epoch advances strictly on every restart (§D14 ghost-writer).
"""

from __future__ import annotations

import os
import shutil
import subprocess
import tempfile

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from omd_server import Coordinator
from omd_server.gitio import GitError

NUM_TASKS = 4
COORD_ID = "fuzz-coord"   # fixed id ⇒ a restart takes over its OWN leader lease (no conflict)


def _git(args, cwd):
    subprocess.run(["git", *args], cwd=str(cwd), check=True, capture_output=True, text=True)


def _init_repo(root: str):
    """root = user HEAD (dev branch); `main` = OMD's dedicated integration branch (§D11)."""
    os.makedirs(root)
    _git(["init", "-b", "main"], root)
    _git(["config", "user.name", "t"], root)
    _git(["config", "user.email", "t@t"], root)
    with open(os.path.join(root, "README.md"), "w") as f:
        f.write("base\n")
    _git(["add", "-A"], root)
    _git(["commit", "-m", "base"], root)
    _git(["checkout", "-b", "dev"], root)


class GitSplitPhaseMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self._tmp = tempfile.mkdtemp(prefix="omd-splitphase-")
        self._repo = os.path.join(self._tmp, "repo")
        self._db = os.path.join(self._tmp, "omd.db")
        self._wt = os.path.join(self._tmp, "wt")
        _init_repo(self._repo)
        self.omd = self._open()
        self.developed: set[str] = set()   # tasks whose work is committed + write-orbit HELD
        # task_id -> "after_a" | "after_b": a mid-connect crash that still holds the merge token.
        # The token is repo-wide (Semaphore max=1) so at most ONE task is ever in flight.
        self.inflight: dict[str, str] = {}

    def _open(self) -> Coordinator:
        return Coordinator(db_path=self._db, repo=self._repo, coordinator_id=COORD_ID,
                           worktrees_dir=self._wt, integration_branch="main",
                           agent_ttl=None)

    def teardown(self):
        try:
            self.omd.store.db.close()
        except Exception:
            pass
        shutil.rmtree(self._tmp, ignore_errors=True)

    # ---------- helpers ----------
    def _t(self, i: int) -> str:
        return f"t{i % NUM_TASKS}"

    def _sub(self, i: int) -> str:
        return f"p{i % NUM_TASKS}"

    def _ag(self, i: int) -> str:
        return f"ag{i % NUM_TASKS}"

    def _glob(self, i: int) -> str:
        return f"{self._sub(i)}/**"

    def _state(self, task: str):
        row = self.omd.store.get_task(task)
        return row["state"] if row else None

    def _held_writes(self, task: str) -> list:
        return [o for o in self.omd.store.orbits_for_task(task)
                if o["mode"] == "write" and o["state"] == "HELD"]

    def _trailer_in_integration(self, task: str):
        wt = self.omd.integration_worktree
        if not wt or not os.path.isdir(wt):
            return None
        return self.omd.git.branch_in_integration(
            wt, self.omd.integration_branch, self.omd._trailer(task))

    def _merge_count(self, task: str) -> int:
        """How many merge commits carry this task's trailer on the integration branch.
        git = the source of truth; this is the anti-double-merge / idempotency probe."""
        out = subprocess.run(
            ["git", "log", "main", "--format=%H", f"--grep=OMD-Connect: {task}"],
            cwd=self._repo, capture_output=True, text=True).stdout
        return len([ln for ln in out.splitlines() if ln.strip()])

    def _connectable(self, task: str) -> bool:
        return (not self.inflight) and task in self.developed and self._state(task) == "DONE"

    # ---------- rules ----------
    @rule(i=st.integers(0, NUM_TASKS - 1))
    def develop(self, i):
        """claim→start→write→commit→finish a fresh task in its own worktree (DONE, lease HELD).
        Allowed even while another task is mid-connect — write-sets are disjoint (§D11)."""
        task = self._t(i)
        if task in self.developed:
            return
        self.omd.declare(task, writes=[self._glob(i)])
        got = self.omd.next_task(self._ag(i))
        # develop is atomic, so `task` is the only PENDING task ⇒ next_task must return it.
        assert got and got["task_id"] == task, (task, got)
        self.omd.claim(self._ag(i), [self._glob(i)], task_id=task)
        s = self.omd.start(task, self._ag(i))
        d = os.path.join(s["worktree"], self._sub(i))
        os.makedirs(d, exist_ok=True)
        with open(os.path.join(d, "f.py"), "w") as f:
            f.write(f"v = {i}\n")
        self.omd.commit(task, f"feat: {self._sub(i)}/f.py")
        self.omd.finish(task)
        assert self._state(task) == "DONE", self._state(task)
        self.developed.add(task)

    @rule(i=st.integers(0, NUM_TASKS - 1))
    def connect_clean(self, i):
        """Full split-phase connect of a disjoint task → MERGED, lease released, token returned."""
        task = self._t(i)
        if not self._connectable(task):
            return
        res = self.omd.connect(task)
        assert res.get("ok") and res["state"] == "MERGED", res
        assert self._state(task) == "MERGED"
        assert self.omd.store.get_task(task)["merge_sha"], "MERGED with no merge_sha"
        assert self._held_writes(task) == [], "MERGED but write lease still held"
        assert self.omd.store.all_held_merge_tokens() == [], "token leak after clean connect"
        assert (os.path.exists(os.path.join(self.omd.integration_worktree,
                                            self._sub(i), "f.py"))), "merged file missing in integration"

    @rule(i=st.integers(0, NUM_TASKS - 1))
    def connect_phase_b_fails(self, i):
        """Phase B (git merge) raises → Phase C rolls back to DONE (retryable), no phantom merge,
        token returned. The task stays developed and re-connectable."""
        task = self._t(i)
        if not self._connectable(task):
            return
        real = self.omd.git.merge_into

        def boom(*a, **k):
            raise GitError("injected merge failure")

        self.omd.git.merge_into = boom
        try:
            a = self.omd._connect_phase_a(task, None, None)
            if not a.get("ok") or a.get("noop"):
                return
            sha, err = self.omd._connect_phase_b(a["intent"])
            assert sha is None and err is not None
            res = self.omd._connect_phase_c(task, a["token_id"], a["intent"], sha, err)
        finally:
            self.omd.git.merge_into = real
        assert res.get("ok") is False and res.get("retryable"), res
        assert self._state(task) == "DONE", self._state(task)
        assert self.omd.store.all_held_merge_tokens() == [], "token leak after rollback"
        assert self._trailer_in_integration(task) is None, "phantom merge after failed Phase B"
        assert self._held_writes(task), "rolled-back task lost its write orbit (not re-connectable)"

    @rule(i=st.integers(0, NUM_TASKS - 1))
    def connect_crash_after_a(self, i):
        """Crash right after Phase A: CONNECTING, merge token held, nothing merged in git.
        Left for a later restart() to roll back."""
        task = self._t(i)
        if not self._connectable(task):
            return
        a = self.omd._connect_phase_a(task, None, None)
        if not a.get("ok") or a.get("noop"):
            return
        assert self._state(task) == "CONNECTING"
        assert len(self.omd.store.all_held_merge_tokens()) == 1, "Phase A did not hold the token"
        assert self._trailer_in_integration(task) is None, "merged in git before Phase B?!"
        self.inflight[task] = "after_a"

    @rule(i=st.integers(0, NUM_TASKS - 1))
    def connect_crash_after_b(self, i):
        """Crash after Phase B succeeds but before Phase C records it: the merge LANDED in git
        (trailer present) while the DB still says CONNECTING. A later restart() must forward it."""
        task = self._t(i)
        if not self._connectable(task):
            return
        a = self.omd._connect_phase_a(task, None, None)
        if not a.get("ok") or a.get("noop"):
            return
        sha, err = self.omd._connect_phase_b(a["intent"])
        assert err is None and sha, (sha, err)
        assert self._state(task) == "CONNECTING"
        assert self._trailer_in_integration(task), "Phase B returned a sha but no trailer in integration"
        self.inflight[task] = "after_b"

    @rule(i=st.integers(0, NUM_TASKS - 1))
    def reconnect_merged_is_noop(self, i):
        """Re-connecting an already-MERGED task is an idempotent no-op — no second merge, no token."""
        task = self._t(i)
        if self.inflight or self._state(task) != "MERGED":
            return
        before = self._merge_count(task)
        res = self.omd.connect(task)
        assert res.get("ok") and res["state"] == "MERGED" and res.get("noop"), res
        assert self._merge_count(task) == before, "idempotent reconnect double-merged"
        assert self.omd.store.all_held_merge_tokens() == []

    @rule()
    def restart(self):
        """Simulate a coordinator crash + restart: close the DB, re-open on the SAME file + repo.
        `_recover()` reconciles every CONNECTING task with git truth (trailer-probe): merged-in-git
        → forward to MERGED; otherwise roll back to DONE. Dangling merge tokens are aborted."""
        old_epoch = self.omd.leader_epoch
        self.omd.store.db.close()
        self.omd = self._open()
        # §D14: a fresh leader takes a strictly higher epoch — no ghost writer.
        assert self.omd.leader_epoch is not None
        if old_epoch is not None:
            assert self.omd.leader_epoch > old_epoch, (old_epoch, self.omd.leader_epoch)

        for task, kind in self.inflight.items():
            stt = self._state(task)
            if kind == "after_b":
                # merge had landed in git → recovery must forward, not re-merge.
                assert stt == "MERGED", (task, kind, stt)
                assert self.omd.store.get_task(task)["merge_sha"], (task, "no merge_sha after forward")
                assert self._held_writes(task) == [], (task, "lease not released on forward")
                assert self._merge_count(task) == 1, (task, "double merge across recovery")
            else:  # after_a
                # nothing merged → recovery must roll back, leaving the task re-connectable.
                assert stt == "DONE", (task, kind, stt)
                assert self._held_writes(task), (task, "rolled-back task lost its write orbit")
                assert self._trailer_in_integration(task) is None, (task, "phantom merge on rollback")
        self.inflight.clear()
        assert self.omd.store.all_held_merge_tokens() == [], "dangling merge token survived recovery"

    # ---------- invariants (after every rule) ----------
    @invariant()
    def at_most_one_merge_token(self):
        assert len(self.omd.store.all_held_merge_tokens()) <= 1

    @invariant()
    def settled_holds_no_token(self):
        if not self.inflight:
            assert self.omd.store.all_held_merge_tokens() == [], \
                "merge token held with nothing in flight (leak)"

    @invariant()
    def merged_matches_git_truth(self):
        for i in range(NUM_TASKS):
            task = self._t(i)
            if self._state(task) != "MERGED":
                continue
            assert self.omd.store.get_task(task)["merge_sha"], (task, "MERGED, no merge_sha")
            assert self._trailer_in_integration(task), (task, "MERGED but trailer absent in git")
            assert self._held_writes(task) == [], (task, "MERGED but write lease held")
            assert self._merge_count(task) == 1, (task, "MERGED but merge count != 1")

    @invariant()
    def integration_worktree_clean(self):
        wt = self.omd.integration_worktree
        if wt and os.path.isdir(wt):
            out = subprocess.run(["git", "status", "--porcelain"], cwd=wt,
                                 capture_output=True, text=True).stdout.strip()
            assert out == "", f"integration worktree dirty (dangling merge?): {out!r}"


# Real git subprocesses per step, so keep the budget modest but meaningful; the
# restart rule makes each example exercise the on-disk reconcile + reconnect path.
GitSplitPhaseMachine.TestCase.settings = settings(
    max_examples=20, stateful_step_count=15, deadline=None,
)
TestGitSplitPhase = GitSplitPhaseMachine.TestCase
