"""P6 — D14 멀티프로세스 HA integration 실측 (증분12).

FEEDBACK §P6 / CONCURRENCY §D14 의 실측 공백: 단일리더 admission·epoch takeover·stale
fence-out 이 전부 **단일 프로세스 안** 두 Coordinator 객체로만 검증돼 있었다. 여기서는
**실제 OS 프로세스**로 검증한다 — SQLite WAL + BEGIN IMMEDIATE 가 프로세스 경계를 넘어
직렬화하는지, epoch fence 가 진짜 프로세스 정지(SIGSTOP=GC-pause 아날로그)·크래시(SIGKILL)
를 가로질러 서는지.

  INV-P6-1 (admission): 리더 A 가 살아있는 동안(heartbeat TTL 내) 두 번째 프로세스 B 의
      기동은 CoordinatorConflict 로 거부된다(한 DB 에 writer 둘 금지).
  INV-P6-2 (crash takeover): A 를 SIGKILL(진짜 크래시) → TTL 경과 → B 기동이 성공하고
      epoch 가 단조 증가(+1)한다(영구 점유 불가).
  INV-P6-3 (GC-pause stale fence-out — Kleppmann 시나리오): A 를 SIGSTOP(프로세스 정지)
      → TTL 경과 → B takeover → SIGCONT 로 A 가 깨어나 변이(claim/heartbeat)를 시도하면
      stale epoch 로 **거부**된다. B 의 변이는 정상.
"""
import os
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

_ROOT = str(Path(__file__).resolve().parents[1])

_DRIVER = textwrap.dedent("""
    import sys
    sys.path.insert(0, sys.argv[4])
    from omd_server import Coordinator, CoordinatorConflict

    db, cid, ttl = sys.argv[1], sys.argv[2], float(sys.argv[3])
    try:
        omd = Coordinator(db_path=db, coordinator_id=cid, leader_ttl=ttl, agent_ttl=None)
    except CoordinatorConflict:
        print("CONFLICT", flush=True)
        sys.exit(3)
    print(f"READY {omd.leader_epoch}", flush=True)
    for line in sys.stdin:
        cmd = line.split()
        if not cmd:
            continue
        try:
            if cmd[0] == "hb":
                r = omd.coordinator_heartbeat()
                print(f"OK hb {r['epoch']}", flush=True)
            elif cmd[0] == "claim":
                r = omd.claim(cmd[2], [cmd[1]], "write")
                print(f"OK claim {r['state']}", flush=True)
            elif cmd[0] == "exit":
                sys.exit(0)
        except CoordinatorConflict:
            print("FENCED", flush=True)
""")


def _spawn(tmp_path, db, cid, ttl):
    return subprocess.Popen(
        [sys.executable, str(tmp_path / "driver.py"), db, cid, str(ttl), _ROOT],
        stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        text=True, bufsize=1)


def _readline(p):
    """driver 는 결정적 1줄-응답 프로토콜 — blocking readline + 프로세스 사망 감지."""
    line = p.stdout.readline().strip()
    assert line, f"driver died: rc={p.poll()} stderr={p.stderr.read()[:800]}"
    return line


def _cmd(p, line):
    p.stdin.write(line + "\n")
    p.stdin.flush()
    return _readline(p)


def _setup(tmp_path):
    (tmp_path / "driver.py").write_text(_DRIVER)
    return str(tmp_path / "o.db")


def _stop(*procs):
    for p in procs:
        if p and p.poll() is None:
            p.kill()
            p.wait(timeout=10)


# ---------------------------------------------------------------------------


def test_second_process_admission_rejected_while_leader_alive(tmp_path):
    """INV-P6-1: 살아있는 리더(A) 옆에서 두 번째 *프로세스*(B) 기동은 CONFLICT 로 거부."""
    db = _setup(tmp_path)
    a = _spawn(tmp_path, db, "coordA", ttl=30.0)
    try:
        assert _readline(a).startswith("READY 1")
        assert _cmd(a, "hb").startswith("OK hb")            # lease 확실히 신선
        b = _spawn(tmp_path, db, "coordB", ttl=30.0)
        try:
            assert _readline(b) == "CONFLICT", "살아있는 리더 옆 2호 기동은 거부돼야(§D14)"
            assert b.wait(timeout=10) == 3
        finally:
            _stop(b)
        assert _cmd(a, "claim a/** agA") == "OK claim HELD", "현직 리더는 계속 정상 변이"
    finally:
        _stop(a)


def test_sigkill_crash_then_takeover_with_epoch_bump(tmp_path):
    """INV-P6-2: 리더 SIGKILL(진짜 크래시) → TTL 경과 → 새 프로세스가 epoch+1 로 takeover."""
    db = _setup(tmp_path)
    a = _spawn(tmp_path, db, "coordA", ttl=1.0)
    assert _readline(a).startswith("READY 1")
    a.kill()                                                # 크래시 — resign 없음
    a.wait(timeout=10)
    time.sleep(1.5)                                         # TTL(1.0s) 경과

    b = _spawn(tmp_path, db, "coordB", ttl=30.0)
    try:
        r = _readline(b)
        assert r == "READY 2", f"죽은 리더는 takeover 대상, epoch 단조 +1 이어야: {r}"
        assert _cmd(b, "claim a/** agB") == "OK claim HELD"
    finally:
        _stop(b)


def test_gc_pause_stale_leader_fenced_out_across_processes(tmp_path):
    """INV-P6-3 (Kleppmann GC-pause): SIGSTOP 으로 정지한 리더 A 위로 B 가 takeover 한 뒤,
    깨어난 A 의 변이(heartbeat/claim)는 stale epoch 로 거부되고 B 는 정상 변이."""
    db = _setup(tmp_path)
    a = _spawn(tmp_path, db, "coordA", ttl=1.0)
    b = None
    try:
        assert _readline(a).startswith("READY 1")
        os.kill(a.pid, signal.SIGSTOP)                      # GC-pause 아날로그
        time.sleep(1.5)                                     # TTL 경과 — lease 죽은 것으로 관측

        b = _spawn(tmp_path, db, "coordB", ttl=30.0)
        assert _readline(b) == "READY 2", "정지된 리더는 죽은 것 — takeover 성립"

        os.kill(a.pid, signal.SIGCONT)                      # A 가 깨어난다(자기가 리더인 줄 앎)
        assert _cmd(a, "hb") == "FENCED", "깨어난 좀비 리더의 heartbeat 는 fence-out"
        assert _cmd(a, "claim a/** agA") == "FENCED", "좀비 리더의 변이는 전부 거부(§D14)"
        assert _cmd(b, "claim a/** agB") == "OK claim HELD", "새 리더의 변이는 정상"
    finally:
        if a.poll() is None:
            try:
                os.kill(a.pid, signal.SIGCONT)
            except ProcessLookupError:
                pass
        _stop(a, b)
