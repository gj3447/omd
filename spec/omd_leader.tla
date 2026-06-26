---- MODULE omd_leader ----
EXTENDS Naturals, TLC

\* D14 leader-lease model.
\*
\* At most one coordinator may be the live writer for a DB epoch. A takeover
\* increments the epoch; stale coordinators may still exist, but every mutation
\* must be fenced by the current (leader, epoch) pair.

CONSTANTS Coordinators, MaxEpoch, MaxWrites

NoCoord == "none"

VARIABLES
  leader,
  epoch,
  localEpoch,
  alive,
  lastWriter,
  lastWriteEpoch,
  writes

vars == << leader, epoch, localEpoch, alive, lastWriter, lastWriteEpoch, writes >>

Init ==
  /\ leader = NoCoord
  /\ epoch = 0
  /\ localEpoch = [c \in Coordinators |-> 0]
  /\ alive = [c \in Coordinators |-> FALSE]
  /\ lastWriter = NoCoord
  /\ lastWriteEpoch = 0
  /\ writes = [c \in Coordinators |-> 0]

Start(c) ==
  /\ leader = NoCoord
  /\ epoch < MaxEpoch
  /\ leader' = c
  /\ epoch' = epoch + 1
  /\ localEpoch' = [localEpoch EXCEPT ![c] = epoch + 1]
  /\ alive' = [alive EXCEPT ![c] = TRUE]
  /\ UNCHANGED << lastWriter, lastWriteEpoch, writes >>

Heartbeat(c) ==
  /\ leader = c
  /\ localEpoch[c] = epoch
  /\ alive[c]
  /\ UNCHANGED vars

Crash(c) ==
  /\ alive[c]
  /\ alive' = [alive EXCEPT ![c] = FALSE]
  /\ UNCHANGED << leader, epoch, localEpoch, lastWriter, lastWriteEpoch, writes >>

Resign(c) ==
  /\ leader = c
  /\ localEpoch[c] = epoch
  /\ leader' = NoCoord
  /\ alive' = [alive EXCEPT ![c] = FALSE]
  /\ UNCHANGED << epoch, localEpoch, lastWriter, lastWriteEpoch, writes >>

Takeover(c) ==
  /\ c \in Coordinators
  /\ c # leader
  /\ leader # NoCoord
  /\ ~alive[leader]
  /\ epoch < MaxEpoch
  /\ leader' = c
  /\ epoch' = epoch + 1
  /\ localEpoch' = [localEpoch EXCEPT ![c] = epoch + 1]
  /\ alive' = [alive EXCEPT ![c] = TRUE]
  /\ UNCHANGED << lastWriter, lastWriteEpoch, writes >>

Mutate(c) ==
  /\ leader = c
  /\ localEpoch[c] = epoch
  /\ alive[c]
  /\ writes[c] < MaxWrites
  /\ lastWriter' = c
  /\ lastWriteEpoch' = epoch
  /\ writes' = [writes EXCEPT ![c] = @ + 1]
  /\ UNCHANGED << leader, epoch, localEpoch, alive >>

\* A stale coordinator waking up is allowed as an environment action, but it
\* does not update localEpoch. Mutate must still reject it.
WakeStale(c) ==
  /\ c # leader
  /\ localEpoch[c] < epoch
  /\ alive' = [alive EXCEPT ![c] = TRUE]
  /\ UNCHANGED << leader, epoch, localEpoch, lastWriter, lastWriteEpoch, writes >>

Next ==
  \/ \E c \in Coordinators: Start(c)
  \/ \E c \in Coordinators: Heartbeat(c)
  \/ \E c \in Coordinators: Crash(c)
  \/ \E c \in Coordinators: Resign(c)
  \/ \E c \in Coordinators: Takeover(c)
  \/ \E c \in Coordinators: Mutate(c)
  \/ \E c \in Coordinators: WakeStale(c)

Spec == Init /\ [][Next]_vars

SingleLeader ==
  leader = NoCoord \/ leader \in Coordinators

NoEnabledStaleMutate ==
  \A c \in Coordinators:
    /\ c # leader
    /\ localEpoch[c] < epoch
    /\ alive[c]
    => ~ENABLED Mutate(c)

StaleCoordinatorCannotBeLeader ==
  \A c \in Coordinators:
    /\ c = leader
    => localEpoch[c] = epoch

TypeOK ==
  /\ leader \in Coordinators \cup {NoCoord}
  /\ epoch \in 0..MaxEpoch
  /\ localEpoch \in [Coordinators -> 0..MaxEpoch]
  /\ alive \in [Coordinators -> BOOLEAN]
  /\ lastWriter \in Coordinators \cup {NoCoord}
  /\ lastWriteEpoch \in 0..MaxEpoch
  /\ writes \in [Coordinators -> 0..MaxWrites]

====
