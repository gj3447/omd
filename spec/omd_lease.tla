---- MODULE omd_lease ----
EXTENDS Naturals, Sequences, TLC

\* A small executable model of the OMD lease/fence core.
\*
\* This intentionally models only the pre-merge coordination layer:
\* claim/release/expire/sweep/start/finish/connect over a finite set of
\* agents, tasks, and write resources. Git, SQLite, wall-clock precision, and
\* path glob matching are abstracted away so TLC can exhaust interleavings.

CONSTANTS Agents, Tasks, Resources

TaskState == {"PENDING", "READY", "IN_ORBIT", "DONE", "MERGED"}
OrbitState == {"HELD", "PENDING", "RELEASED", "EXPIRED"}

VARIABLES
  taskState,
  taskAgent,
  taskWrites,
  orbitState,
  orbitAgent,
  orbitTask,
  orbitRes,
  orbitFence,
  nextFence

vars == << taskState, taskAgent, taskWrites,
          orbitState, orbitAgent, orbitTask, orbitRes, orbitFence, nextFence >>

TaskOrbits(t) == {o \in DOMAIN orbitState:
  orbitTask[o] = t /\ orbitState[o] \in {"HELD", "PENDING"}}

HeldOrbits == {o \in DOMAIN orbitState: orbitState[o] = "HELD"}

Overlaps(r1, r2) == r1 = r2

Conflict(res) ==
  \E o \in DOMAIN orbitState:
    orbitState[o] = "HELD" /\ Overlaps(orbitRes[o], res)

Init ==
  /\ taskState = [t \in Tasks |-> "PENDING"]
  /\ taskAgent = [t \in Tasks |-> CHOOSE a \in Agents: TRUE]
  /\ taskWrites \in [Tasks -> Resources]
  /\ orbitState = [o \in (Agents \X Resources) |-> "RELEASED"]
  /\ orbitAgent = [o \in (Agents \X Resources) |-> o[1]]
  /\ orbitTask = [o \in (Agents \X Resources) |-> CHOOSE t \in Tasks: TRUE]
  /\ orbitRes = [o \in (Agents \X Resources) |-> o[2]]
  /\ orbitFence = [o \in (Agents \X Resources) |-> 0]
  /\ nextFence = 1

Claim(a, t) ==
  LET res == taskWrites[t]
      oid == <<a, res>>
  IN
  /\ taskState[t] \in {"PENDING", "READY"}
  /\ orbitState[oid] \notin {"HELD", "PENDING"}
  /\ IF Conflict(res)
        THEN /\ orbitState' = [orbitState EXCEPT ![oid] = "PENDING"]
             /\ orbitFence' = orbitFence
             /\ nextFence' = nextFence
        ELSE /\ orbitState' = [orbitState EXCEPT ![oid] = "HELD"]
             /\ orbitFence' = [orbitFence EXCEPT ![oid] = nextFence]
             /\ nextFence' = nextFence + 1
  /\ orbitTask' = [orbitTask EXCEPT ![oid] = t]
  /\ taskState' = [taskState EXCEPT ![t] = "READY"]
  /\ taskAgent' = [taskAgent EXCEPT ![t] = a]
  /\ UNCHANGED << taskWrites, orbitAgent, orbitRes >>

Release(o) ==
  /\ orbitState[o] = "HELD"
  /\ orbitState' = [orbitState EXCEPT ![o] = "RELEASED"]
  /\ UNCHANGED << taskState, taskAgent, taskWrites,
                  orbitAgent, orbitTask, orbitRes, orbitFence, nextFence >>

Expire(o) ==
  /\ orbitState[o] = "HELD"
  /\ orbitState' = [orbitState EXCEPT ![o] = "EXPIRED"]
  /\ IF taskState[orbitTask[o]] = "IN_ORBIT"
        THEN taskState' = [taskState EXCEPT ![orbitTask[o]] = "PENDING"]
        ELSE taskState' = taskState
  /\ UNCHANGED << taskAgent, taskWrites,
                  orbitAgent, orbitTask, orbitRes, orbitFence, nextFence >>

Promote(o) ==
  /\ orbitState[o] = "PENDING"
  /\ ~Conflict(orbitRes[o])
  /\ orbitState' = [orbitState EXCEPT ![o] = "HELD"]
  /\ orbitFence' = [orbitFence EXCEPT ![o] = nextFence]
  /\ nextFence' = nextFence + 1
  /\ UNCHANGED << taskState, taskAgent, taskWrites,
                  orbitAgent, orbitTask, orbitRes >>

Start(t) ==
  /\ taskState[t] = "READY"
  /\ \E o \in DOMAIN orbitState:
       orbitTask[o] = t /\ orbitState[o] = "HELD"
  /\ taskState' = [taskState EXCEPT ![t] = "IN_ORBIT"]
  /\ UNCHANGED << taskAgent, taskWrites, orbitState,
                  orbitAgent, orbitTask, orbitRes, orbitFence, nextFence >>

Finish(t) ==
  /\ taskState[t] = "IN_ORBIT"
  /\ taskState' = [taskState EXCEPT ![t] = "DONE"]
  /\ UNCHANGED << taskAgent, taskWrites, orbitState,
                  orbitAgent, orbitTask, orbitRes, orbitFence, nextFence >>

Connect(t) ==
  /\ taskState[t] = "DONE"
  /\ \E o \in DOMAIN orbitState:
       orbitTask[o] = t /\ orbitState[o] = "HELD" /\ orbitFence[o] > 0
  /\ taskState' = [taskState EXCEPT ![t] = "MERGED"]
  /\ orbitState' = [o \in DOMAIN orbitState |->
       IF orbitTask[o] = t /\ orbitState[o] = "HELD"
       THEN "RELEASED"
       ELSE orbitState[o]]
  /\ UNCHANGED << taskAgent, taskWrites,
                  orbitAgent, orbitTask, orbitRes, orbitFence, nextFence >>

Next ==
  \/ \E a \in Agents, t \in Tasks: Claim(a, t)
  \/ \E o \in DOMAIN orbitState: Release(o)
  \/ \E o \in DOMAIN orbitState: Expire(o)
  \/ \E o \in DOMAIN orbitState: Promote(o)
  \/ \E t \in Tasks: Start(t)
  \/ \E t \in Tasks: Finish(t)
  \/ \E t \in Tasks: Connect(t)

Spec == Init /\ [][Next]_vars

NoOverlappingHeld ==
  \A o1, o2 \in DOMAIN orbitState:
    /\ o1 # o2
    /\ orbitState[o1] = "HELD"
    /\ orbitState[o2] = "HELD"
    => ~Overlaps(orbitRes[o1], orbitRes[o2])

UniqueLiveFence ==
  \A o1, o2 \in DOMAIN orbitState:
    /\ o1 # o2
    /\ orbitState[o1] = "HELD"
    /\ orbitState[o2] = "HELD"
    => orbitFence[o1] # orbitFence[o2]

NoConnectBeforeDone ==
  \A t \in Tasks:
    taskState[t] = "MERGED" => orbitState[<<taskAgent[t], taskWrites[t]>>] = "RELEASED"

TypeOK ==
  /\ taskState \in [Tasks -> TaskState]
  /\ taskAgent \in [Tasks -> Agents]
  /\ taskWrites \in [Tasks -> Resources]
  /\ orbitState \in [(Agents \X Resources) -> OrbitState]
  /\ orbitFence \in [(Agents \X Resources) -> Nat]
  /\ nextFence \in Nat

====
