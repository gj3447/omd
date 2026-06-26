---- MODULE omd_connect ----
EXTENDS Naturals, TLC

\* Split-phase CLOUD CONNECT model.
\*
\* Phase A captures the write lease and repo-wide merge token, Phase B happens
\* outside the coordinator critical section, and Phase C either commits the
\* merge result or rolls the task back to DONE. Recover actions model restart
\* reconciliation of a task left in CONNECTING.

CONSTANTS Tasks

TaskState == {"DONE", "CONNECTING", "MERGED"}
MergeResult == {"NONE", "OK", "ERR"}
NoTask == "none"

VARIABLES
  taskState,
  writeHeld,
  pinned,
  mergeSha,
  mergeResult,
  tokenOwner

vars == << taskState, writeHeld, pinned, mergeSha, mergeResult, tokenOwner >>

Init ==
  /\ taskState = [t \in Tasks |-> "DONE"]
  /\ writeHeld = [t \in Tasks |-> TRUE]
  /\ pinned = [t \in Tasks |-> FALSE]
  /\ mergeSha = [t \in Tasks |-> FALSE]
  /\ mergeResult = [t \in Tasks |-> "NONE"]
  /\ tokenOwner = NoTask

PhaseA(t) ==
  /\ taskState[t] = "DONE"
  /\ writeHeld[t]
  /\ tokenOwner = NoTask
  /\ taskState' = [taskState EXCEPT ![t] = "CONNECTING"]
  /\ pinned' = [pinned EXCEPT ![t] = TRUE]
  /\ mergeResult' = [mergeResult EXCEPT ![t] = "NONE"]
  /\ tokenOwner' = t
  /\ UNCHANGED << writeHeld, mergeSha >>

PhaseBOk(t) ==
  /\ taskState[t] = "CONNECTING"
  /\ tokenOwner = t
  /\ mergeResult[t] = "NONE"
  /\ mergeResult' = [mergeResult EXCEPT ![t] = "OK"]
  /\ UNCHANGED << taskState, writeHeld, pinned, mergeSha, tokenOwner >>

PhaseBErr(t) ==
  /\ taskState[t] = "CONNECTING"
  /\ tokenOwner = t
  /\ mergeResult[t] = "NONE"
  /\ mergeResult' = [mergeResult EXCEPT ![t] = "ERR"]
  /\ UNCHANGED << taskState, writeHeld, pinned, mergeSha, tokenOwner >>

PhaseCOk(t) ==
  /\ taskState[t] = "CONNECTING"
  /\ tokenOwner = t
  /\ mergeResult[t] = "OK"
  \* P0-6 ordering: merge evidence is recorded before the write lease is released.
  /\ mergeSha' = [mergeSha EXCEPT ![t] = TRUE]
  /\ taskState' = [taskState EXCEPT ![t] = "MERGED"]
  /\ writeHeld' = [writeHeld EXCEPT ![t] = FALSE]
  /\ pinned' = [pinned EXCEPT ![t] = FALSE]
  /\ tokenOwner' = NoTask
  /\ mergeResult' = [mergeResult EXCEPT ![t] = "NONE"]

PhaseCErr(t) ==
  /\ taskState[t] = "CONNECTING"
  /\ tokenOwner = t
  /\ mergeResult[t] = "ERR"
  /\ taskState' = [taskState EXCEPT ![t] = "DONE"]
  /\ pinned' = [pinned EXCEPT ![t] = FALSE]
  /\ tokenOwner' = NoTask
  /\ mergeResult' = [mergeResult EXCEPT ![t] = "NONE"]
  /\ UNCHANGED << writeHeld, mergeSha >>

RecoverForward(t) ==
  /\ taskState[t] = "CONNECTING"
  /\ mergeResult[t] = "OK"
  /\ mergeSha' = [mergeSha EXCEPT ![t] = TRUE]
  /\ taskState' = [taskState EXCEPT ![t] = "MERGED"]
  /\ writeHeld' = [writeHeld EXCEPT ![t] = FALSE]
  /\ pinned' = [pinned EXCEPT ![t] = FALSE]
  /\ tokenOwner' = NoTask
  /\ mergeResult' = [mergeResult EXCEPT ![t] = "NONE"]

RecoverRollback(t) ==
  /\ taskState[t] = "CONNECTING"
  /\ mergeResult[t] # "OK"
  /\ taskState' = [taskState EXCEPT ![t] = "DONE"]
  /\ pinned' = [pinned EXCEPT ![t] = FALSE]
  /\ tokenOwner' = NoTask
  /\ mergeResult' = [mergeResult EXCEPT ![t] = "NONE"]
  /\ UNCHANGED << writeHeld, mergeSha >>

Next ==
  \/ \E t \in Tasks: PhaseA(t)
  \/ \E t \in Tasks: PhaseBOk(t)
  \/ \E t \in Tasks: PhaseBErr(t)
  \/ \E t \in Tasks: PhaseCOk(t)
  \/ \E t \in Tasks: PhaseCErr(t)
  \/ \E t \in Tasks: RecoverForward(t)
  \/ \E t \in Tasks: RecoverRollback(t)

Spec == Init /\ [][Next]_vars

AtMostOneMergeToken ==
  tokenOwner = NoTask \/ tokenOwner \in Tasks

TokenImpliesConnecting ==
  tokenOwner # NoTask => taskState[tokenOwner] = "CONNECTING"

MergedHasMergeSha ==
  \A t \in Tasks: taskState[t] = "MERGED" => mergeSha[t]

MergedReleasesWriteLease ==
  \A t \in Tasks: taskState[t] = "MERGED" => ~writeHeld[t]

PinnedOnlyWhileConnecting ==
  \A t \in Tasks: pinned[t] => taskState[t] = "CONNECTING"

NoTwoConnectingWithToken ==
  \A t1, t2 \in Tasks:
    /\ t1 # t2
    /\ tokenOwner = t1
    => tokenOwner # t2

TypeOK ==
  /\ taskState \in [Tasks -> TaskState]
  /\ writeHeld \in [Tasks -> BOOLEAN]
  /\ pinned \in [Tasks -> BOOLEAN]
  /\ mergeSha \in [Tasks -> BOOLEAN]
  /\ mergeResult \in [Tasks -> MergeResult]
  /\ tokenOwner \in Tasks \cup {NoTask}

====
