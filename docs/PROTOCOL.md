# Basic HotStuff protocol walkthrough

This document maps the simulator to **Basic HotStuff**. The implementation intentionally keeps the explicit `PREPARE`, `PRECOMMIT`, `COMMIT`, and `DECIDE` phases visible instead of implementing Chained HotStuff.

## Model

The replica set is fixed and permissioned. For up to `f` Byzantine replicas, configuration enforces:

```text
n >= 3f + 1
quorum = n - f
```

When `n = 3f + 1`, the quorum is `2f + 1`.

The simulator models authenticated reliable point-to-point links. Every replica has a bound endpoint that can authenticate messages only as that replica. Broadcast is expanded into one point-to-point message per recipient. A transient `Drop` decision causes retransmission rather than permanent loss; delay functions model pre-GST and post-GST timing behavior.

## Compact threshold-authenticator model

Every replica has an individual vote-share key inside an in-process threshold oracle. A vote share authenticates:

```text
<phase, view, nodeID>
```

The oracle's `Combine` operation checks:

1. every share is valid for its claimed voter;
2. every voter belongs to the fixed replica set;
3. voters are unique;
4. all shares bind the same phase, view, and node;
5. at least `n-f` valid shares are present.

Only then does it issue one fixed-size aggregate authenticator. A QC stores the tuple and this single aggregate; it does not store all votes.

The oracle is an educational stand-in for a real threshold-signature scheme. It models compact QCs and unforgeability at the protocol boundary, but it is not production BLS/RSA threshold cryptography.

## Data structures

### Node

A node contains:

- full SHA-256 ID;
- parent ID;
- command;
- height;
- proposer;
- view number;
- locally derived ancestry.

A received node is accepted only when its hash recomputes correctly, its parent exists, its height follows its parent, and its proposer/view match the expected leader/view. Ancestry supplied by a sender is discarded and reconstructed from local parent links.

### Vote

A vote contains:

- voter ID;
- phase;
- view;
- node ID;
- partial authenticator.

Correct replicas do not vote twice for conflicting nodes in the same phase/view. A receiving leader additionally checks that the authenticated message sender equals the vote's claimed voter.

### QC

A quorum certificate contains:

- phase;
- view;
- node ID;
- one aggregate authenticator.

Validation uses the locally configured quorum. The QC cannot choose its own threshold.

## Phase-by-phase Basic HotStuff

### 1. NEW_VIEW

When a replica enters view `v`, it sends:

```text
NEW_VIEW(
    view = v,
    justify = highest prepareQC,
    branch = branch through prepareQC.node
)
```

The leader accepts the message only if:

- the sender is a configured replica;
- the message authentication tag is valid;
- the QC aggregate is valid for the configured quorum;
- the QC is genesis or a `prepareQC` from a lower view;
- the supplied branch validates and contains the referenced node.

After `n-f` valid `NEW_VIEW` messages, the leader chooses the highest-view `prepareQC` as `highQC`.

### 2. PREPARE

The leader creates a node extending `highQC.node` and broadcasts:

```text
PREPARE(node, justify = highQC, branch = branch through highQC.node)
```

A replica:

1. validates/imports the parent branch;
2. verifies the node hash, parent, height, proposer, and view;
3. verifies that the node extends `justify.node`;
4. applies `safeNode`;
5. enforces one `PREPARE` vote per view;
6. inserts the node only after all checks pass;
7. sends an authenticated partial vote to the leader.

The Basic HotStuff voting rule is:

```text
node extends lockedQC.node
OR
justifyQC.view > lockedQC.view
```

### 3. PRECOMMIT

After `n-f` valid `PREPARE` shares, the leader creates one compact `prepareQC` and broadcasts:

```text
PRECOMMIT(view = v, justify = prepareQC(view = v), branch)
```

A replica requires an exact phase and view match. A stale `prepareQC` from another view cannot advance the current view's phase. After validation, the replica stores `PrepareQC` and sends a `PRECOMMIT` vote.

### 4. COMMIT

After `n-f` valid `PRECOMMIT` shares, the leader creates `precommitQC` and broadcasts:

```text
COMMIT(view = v, justify = precommitQC(view = v), branch)
```

A replica validates the exact phase/view, updates:

```text
lockedQC = precommitQC
```

and sends a `COMMIT` vote.

### 5. DECIDE

After `n-f` valid `COMMIT` shares, the leader creates `commitQC` and broadcasts:

```text
DECIDE(view = v, justify = commitQC(view = v), branch)
```

Replicas validate the commit QC and execute all newly decided nodes from the previously executed prefix through `commitQC.node`.

## Missing-node handling

The old behavior of silently replacing an unavailable `highQC.node` with genesis is forbidden. Branches are attached to protocol messages and validated before use. If the highest QC's node remains unavailable, the leader does not propose in that view.

## Pacemaker and partial synchrony

The Pacemaker uses round-robin leaders and exponential timeout backoff:

```text
timeout_0 = configured initial timeout
timeout_(k+1) = min(2 * timeout_k, maximum timeout)
```

After a decision, timeout returns to the configured initial value. This models the standard liveness argument: after GST, timeout intervals eventually become long enough for correct replicas to overlap in a view led by a correct leader.

Safety checks never depend on timeout expiration.

## Byzantine tests

The test suite exercises more than simple equivocation:

- a leader fabricates an aggregate QC without shares;
- a replica attempts sender/voter impersonation;
- duplicate and mismatched vote shares are combined;
- a stale phase QC is reused in a later view;
- a node lies about its ancestry;
- a node payload is changed after hashing;
- a leader has a valid high QC whose node is unavailable;
- the network transiently drops authenticated messages;
- the Pacemaker timeout doubles and caps correctly.

The safety expectation remains:

```text
No two correct replicas decide conflicting branches.
```

The liveness expectation is conditional on eventual network stability and a correct leader remaining active long enough.
