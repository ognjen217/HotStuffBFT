# Basic HotStuff protocol walkthrough

This document explains the simulator protocol in educational terms. The implementation intentionally follows Basic HotStuff rather than Chained HotStuff so the phases are visible in the trace.

## Model

The simulator uses a fixed permissioned replica set. For `f` Byzantine replicas, the system needs at least `n = 3f + 1` total replicas. A quorum certificate needs `n - f = 2f + 1` votes. With the default `n=4`, `f=1`, a QC needs `3` unique voters.

Network communication is in-memory, asynchronous, and can be delayed or dropped by scenario rules. This lets the tests model partial synchrony: safety should hold during delay and fault periods, while liveness is expected after the network stabilizes and a correct leader has enough time.

## Data structures

### Node

A node wraps one banking command and points to its parent. Its branch is the path from genesis to that node.

Relevant fields:

- `id/hash`
- `parent id/hash`
- `command`
- `height`
- `proposer/leader`
- `view number`

### Vote

A vote records:

- voter id,
- phase,
- view,
- node id.

Correct replicas do not vote twice for conflicting nodes in the same phase and view.

### QC

A QC is a simulated threshold certificate. It contains the phase, view, node id, voters, and the votes that justify it. A QC is valid when at least quorum unique voters voted for the same phase, same view, and same node.

The simulator does not implement real threshold cryptography. It models the semantics only.

## Phase-by-phase Basic HotStuff

### 1. New-view

When a replica enters view `v`, it sends:

```text
NEW_VIEW(view=v, justify=highest prepareQC)
```

to `leader(v)`.

The leader waits for `n-f` new-view messages. From these, it selects `highQC`, the highest-view `prepareQC`. If no previous QC exists, it uses `genesisQC`.

### 2. Prepare

The leader creates a new node extending `highQC.node` and broadcasts:

```text
PREPARE(node, justify=highQC)
```

A replica accepts the proposal only if:

1. the node extends `justify.node`, and
2. `safeNode(node, justifyQC, lockedQC)` is true.

Then it sends a `PREPARE` vote to the leader.

### 3. Pre-commit

The leader collects `n-f` `PREPARE` votes and forms:

```text
prepareQC(node)
```

It broadcasts:

```text
PRECOMMIT(justify=prepareQC)
```

Replicas store `prepareQC` and send `PRECOMMIT` votes.

### 4. Commit

The leader collects `n-f` `PRECOMMIT` votes and forms:

```text
precommitQC(node)
```

It broadcasts:

```text
COMMIT(justify=precommitQC)
```

Replicas set:

```text
lockedQC = precommitQC
```

and send `COMMIT` votes.

### 5. Decide

The leader collects `n-f` `COMMIT` votes and forms:

```text
commitQC(node)
```

It broadcasts:

```text
DECIDE(justify=commitQC)
```

Replicas execute every newly decided command on the branch through `commitQC.node`.

## Banking walkthrough

Initial state:

```text
Marko = 1000
Ana   = 200
Luka  = 100
```

Commands:

```text
b128: BLOCK_ACCOUNT(Marko, reason="fraud-check")
b129: TRANSFER(Marko, Ana, 500)
b130: APPROVE_LOAN(Ana, loanId="L-42", amount=2000)
```

If the replicas decide this order, execution is deterministic:

1. `b128` marks Marko as blocked.
2. `b129` is decided in the ledger but rejected during execution because Marko is blocked.
3. `b130` approves Ana's loan.

All correct replicas therefore have the same ledger and the same final banking state.

## Happy path flow

With a correct leader and no network delay:

```text
NEW_VIEW quorum -> PREPARE proposal -> prepareQC -> precommitQC -> lockedQC -> commitQC -> DECIDE
```

Each view decides one banking command.

## Byzantine equivocation flow

A Byzantine leader can send conflicting proposals in the same view:

```text
B2 receives BLOCK_ACCOUNT(Marko)
B3 receives BLOCK_ACCOUNT(Marko), then conflicting TRANSFER(Marko,Luka)
B4 receives TRANSFER(Marko,Luka)
```

A correct replica that already voted `PREPARE` in that view rejects the later conflicting proposal. Because a valid QC needs `2f+1` votes and there are only `f` Byzantine replicas, two conflicting QCs cannot both be formed from correct voting behavior.

The scenario trace makes this visible with lines such as:

```text
[B3] safeNode rejected ... reason=already voted conflicting PREPARE
```

## Safety intuition

Safety comes from quorum intersection and locking.

Any two quorums of size `2f+1` in a system of `3f+1` replicas intersect in at least `f+1` replicas. Since at most `f` are Byzantine, the intersection contains at least one correct replica. A correct replica does not vote twice for conflicting nodes in the same phase and view, and once locked, it only votes for a conflicting branch if the proposal carries a higher-view QC. This prevents two conflicting branches from both being decided by correct replicas.

## Liveness intuition

The simple pacemaker uses view timeouts and round-robin leaders. If a leader is silent or the network is too slow, replicas timeout and enter the next view. After a GST-like stabilization period, messages arrive before timeout and eventually a correct leader collects enough `NEW_VIEW` messages, proposes a safe node, and drives the phase sequence to `DECIDE`.

## Limitations

This is a teaching simulator. It deliberately omits:

- real signatures and threshold cryptography,
- persistent storage,
- recovery after crashes,
- real client networking,
- batching and mempool logic,
- Chained HotStuff pipelining,
- production-grade pacemaker logic,
- performance engineering.

The goal is readable protocol behavior and tests, not deployment readiness.
