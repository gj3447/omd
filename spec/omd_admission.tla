---- MODULE omd_admission ----
EXTENDS FiniteSets, Naturals, TLC

\* Bounded model of the M1 fair-admission kernel.  Paths are finite sets;
\* overlap is non-empty intersection.  The model covers initial admission,
\* durable queue order, compatible modes, release, and promotion.  Reservation
\* cycle denial and bounded deadlines are outside this bounded model and are
\* covered by executable runtime tests.

CONSTANTS R1, R2, R3, P1, P2,
          Priority1, Priority2, Priority3,
          Mode1, Mode2, Mode3, MaxFence

Requests == {R1, R2, R3}
Paths == {P1, P2}
States == {"NEW", "PENDING", "HELD", "RELEASED"}
Modes == {"read", "write", "shared"}
MaxRequests == Cardinality(Requests)

ASSUME MaxFence >= MaxRequests

RequestPaths(r) ==
  CASE r = R1 -> {P1}
    [] r = R2 -> {P1, P2}
    [] OTHER  -> {P2}

Priority(r) ==
  CASE r = R1 -> Priority1
    [] r = R2 -> Priority2
    [] OTHER  -> Priority3

Mode(r) ==
  CASE r = R1 -> Mode1
    [] r = R2 -> Mode2
    [] OTHER  -> Mode3

Compatible(left, right) ==
  \/ left = "read" /\ right = "read"
  \/ left = "shared" /\ right = "shared"

Conflicts(left, right) ==
  /\ ~Compatible(Mode(left), Mode(right))
  /\ RequestPaths(left) \cap RequestPaths(right) # {}

VARIABLES state, queueSeq, enqueuedAt, grantedAt, fence, nextSeq, nextFence, clock

vars == <<state, queueSeq, enqueuedAt, grantedAt, fence,
          nextSeq, nextFence, clock>>

HigherRank(existing, candidate, candidateSeq) ==
  \/ Priority(existing) > Priority(candidate)
  \/ /\ Priority(existing) = Priority(candidate)
     /\ queueSeq[existing] < candidateSeq

HeldBlocker(r) ==
  \E other \in Requests:
    state[other] = "HELD" /\ Conflicts(other, r)

PendingPredecessorAt(r, candidateSeq) ==
  \E other \in Requests:
    /\ state[other] = "PENDING"
    /\ Conflicts(other, r)
    /\ HigherRank(other, r, candidateSeq)

PendingPredecessor(r) == PendingPredecessorAt(r, queueSeq[r])

Init ==
  /\ state = [r \in Requests |-> "NEW"]
  /\ queueSeq = [r \in Requests |-> 0]
  /\ enqueuedAt = [r \in Requests |-> 0]
  /\ grantedAt = [r \in Requests |-> 0]
  /\ fence = [r \in Requests |-> 0]
  /\ nextSeq = 1
  /\ nextFence = 1
  /\ clock = 0

Claim(r) ==
  LET candidateSeq == nextSeq
      blocked == HeldBlocker(r) \/ PendingPredecessorAt(r, candidateSeq)
  IN
  /\ state[r] = "NEW"
  /\ nextSeq <= MaxRequests
  /\ queueSeq' = [queueSeq EXCEPT ![r] = candidateSeq]
  /\ nextSeq' = nextSeq + 1
  /\ clock' = clock + 1
  /\ IF blocked
        THEN /\ state' = [state EXCEPT ![r] = "PENDING"]
             /\ enqueuedAt' = [enqueuedAt EXCEPT ![r] = clock + 1]
             /\ UNCHANGED <<grantedAt, fence, nextFence>>
        ELSE /\ nextFence <= MaxFence
             /\ state' = [state EXCEPT ![r] = "HELD"]
             /\ grantedAt' = [grantedAt EXCEPT ![r] = clock + 1]
             /\ fence' = [fence EXCEPT ![r] = nextFence]
             /\ nextFence' = nextFence + 1
             /\ UNCHANGED enqueuedAt

Release(r) ==
  /\ state[r] = "HELD"
  /\ state' = [state EXCEPT ![r] = "RELEASED"]
  /\ clock' = clock + 1
  /\ UNCHANGED <<queueSeq, enqueuedAt, grantedAt, fence, nextSeq, nextFence>>

Promote(r) ==
  /\ state[r] = "PENDING"
  /\ ~HeldBlocker(r)
  /\ ~PendingPredecessor(r)
  /\ nextFence <= MaxFence
  /\ state' = [state EXCEPT ![r] = "HELD"]
  /\ grantedAt' = [grantedAt EXCEPT ![r] = clock + 1]
  /\ fence' = [fence EXCEPT ![r] = nextFence]
  /\ nextFence' = nextFence + 1
  /\ clock' = clock + 1
  /\ UNCHANGED <<queueSeq, enqueuedAt, nextSeq>>

ClaimAny == \E r \in Requests: Claim(r)
ReleaseAny == \E r \in Requests: Release(r)
PromoteAny == \E r \in Requests: Promote(r)

Next == ClaimAny \/ ReleaseAny \/ PromoteAny

Spec ==
  /\ Init
  /\ [][Next]_vars
  /\ \A r \in Requests: WF_vars(Release(r))
  /\ \A r \in Requests: WF_vars(Promote(r))

TypeOK ==
  /\ state \in [Requests -> States]
  /\ queueSeq \in [Requests -> 0..MaxRequests]
  /\ enqueuedAt \in [Requests -> Nat]
  /\ grantedAt \in [Requests -> Nat]
  /\ fence \in [Requests -> 0..MaxFence]
  /\ nextSeq \in 1..(MaxRequests + 1)
  /\ nextFence \in 1..(MaxFence + 1)
  /\ clock \in Nat
  /\ Mode1 \in Modes /\ Mode2 \in Modes /\ Mode3 \in Modes

NoIncompatibleOverlap ==
  \A left, right \in Requests:
    /\ left # right
    /\ state[left] = "HELD"
    /\ state[right] = "HELD"
    => ~Conflicts(left, right)

UniqueLiveFence ==
  \A left, right \in Requests:
    /\ left # right
    /\ state[left] = "HELD"
    /\ state[right] = "HELD"
    => fence[left] # fence[right]

\* A pending predecessor that existed before a later grant may not have been
\* bypassed.  A higher-priority request arriving after an existing grant is not
\* retroactively classified as an overtake.
NoLowerRankOvertaking ==
  \A pending, held \in Requests:
    /\ state[pending] = "PENDING"
    /\ state[held] = "HELD"
    /\ Conflicts(pending, held)
    /\ HigherRank(pending, held, queueSeq[held])
    /\ enqueuedAt[pending] < grantedAt[held]
    => FALSE

PendingResolves ==
  \A r \in Requests:
    state[r] = "PENDING" ~> state[r] \in {"HELD", "RELEASED"}

====
