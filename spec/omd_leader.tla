---- MODULE omd_leader ----
EXTENDS Naturals, TLC

\* D14 leader-lease model.
\*
\* At most one coordinator may be the live writer for a DB epoch. A takeover
\* increments the epoch; stale coordinators may still exist, but every mutation
\* must be fenced by the current (leader, epoch) pair.

CONSTANTS Coordinators

NoCoord == "none"

VARIABLES
  leader,
  epoch,
  localEpoch,
  alive,
  lastWriter,
  writes

vars == << leader, epoch, localEpoch, alive, lastWriter, writes >>

Init ==
  /\ leader = NoCoord
  /\ epoch = 0
  /\ localEpoch = [c \in Coordinators |-> 0]
  /\ alive = [c \in Coordinators |-> FALSE]
  /\ lastWriter = NoCoord
  /\ writes = [c \in Coordinators |-> 0]

Start(c) ==
  /\ leader = NoCoord
  /\ leader' = c
  /\ epoch' = epoch + 1
  /\ localEpoch' = [localEpoch EXCEPT ![c] = epoch + 1]
  /\ alive' = [alive EXCEPT ![c] = TRUE]
  /\ UNCHANGED << lastWriter, writes >>

Heartbeat(c) ==
  /\ leader = c
  /\ localEpoch[c] = epoch
  /\ alive[c]
  /\ UNCHANGED vars

Crash(c) ==
  /\ alive[c]
  /\ alive' = [alive EXCEPT ![c] = FALSE]
  /\ UNCHANGED << leader, epoch, localEpoch, lastWriter, writes >>

Resign(c) ==
  /\ leader = c
  /\ localEpoch[c] = epoch
  /\ leader' = NoCoord
  /\ alive' = [alive EXCEPT ![c] = FALSE]
  /\ UNCHANGED << epoch, localEpoch, lastWriter, writes >>

Takeover(c) ==
  /\ c \in Coordinators
  /\ c # leader
  /\ leader # NoCoord
  /\ ~alive[leader]
  /\ leader' = c
  /\ epoch' = epoch + 1
  /\ localEpoch' = [localEpoch EXCEPT ![c] = epoch + 1]
  /\ alive' = [alive EXCEPT ![c] = TRUE]
  /\ UNCHANGED << lastWriter, writes >>

Mutate(c) ==
  /\ leader = c
  /\ localEpoch[c] = epoch
  /\ alive[c]
  /\ lastWriter' = c
  /\ writes' = [writes EXCEPT ![c] = @ + 1]
  /\ UNCHANGED << leader, epoch, localEpoch, alive >>

\* A stale coordinator waking up is allowed as an environment action, but it
\* does not update localEpoch. Mutate must still reject it.
WakeStale(c) ==
  /\ c # leader
  /\ localEpoch[c] < epoch
  /\ alive' = [alive EXCEPT ![c] = TRUE]
  /\ UNCHANGED << leader, epoch, localEpoch, lastWriter, writes >>

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

LastWriterWasCurrentLeader ==
  lastWriter = NoCoord \/
    /\ lastWriter = leader
    /\ localEpoch[lastWriter] = epoch

StaleCoordinatorCannotBeLeader ==
  \A c \in Coordinators:
    /\ c = leader
    => localEpoch[c] = epoch

TypeOK ==
  /\ leader \in Coordinators \cup {NoCoord}
  /\ epoch \in Nat
  /\ localEpoch \in [Coordinators -> Nat]
  /\ alive \in [Coordinators -> BOOLEAN]
  /\ lastWriter \in Coordinators \cup {NoCoord}
  /\ writes \in [Coordinators -> Nat]

====
