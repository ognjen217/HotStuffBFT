# HotStuffBFT

Educational Go simulator for **Basic HotStuff BFT consensus** using a concrete permissioned banking-network case study.

This is not a production blockchain. It is a clear, testable simulator for learning how replicas use views, leaders, compact quorum certificates, locks, authenticated links, and the Basic HotStuff phases to agree on a deterministic ledger order even when one replica is faulty.

## Banking case study

The replicated service is a small banking ledger. Replicas are banks/regulators that must agree on the order of critical commands:

- `TRANSFER(from, to, amount)`
- `BLOCK_ACCOUNT(accountId, reason)`
- `APPROVE_LOAN(accountId, loanId, amount)`

Default initial accounts:

```text
Marko: 1000
Ana:   200
Luka:  100
```

Default commands:

```text
b128: BLOCK_ACCOUNT(Marko, reason="fraud-check")
b129: TRANSFER(Marko, Ana, 500)
b130: APPROVE_LOAN(Ana, loanId="L-42", amount=2000)
```

Consensus agrees on command order. The deterministic banking state machine then determines whether a decided command is valid. If `b128` is decided before `b129`, every correct replica includes the transfer in the decided ledger but rejects it during execution because Marko is already blocked.

## Run

```bash
go run ./cmd/hotstuff-sim --scenario happy
go run ./cmd/hotstuff-sim --scenario silent-leader
go run ./cmd/hotstuff-sim --scenario byzantine-equivocation
go run ./cmd/hotstuff-sim --scenario byzantine-forged-qc
go run ./cmd/hotstuff-sim --scenario banking-block-transfer
go run ./cmd/hotstuff-sim --scenario delayed-network
```

Optional flags:

```bash
--n 4
--f 1
--timeout-ms 150
--seed 1
--verbose
--log-dir logs
--viz-dir visualizations
--visualize=true
```

`--timeout-ms` is the **initial** view timeout. The Pacemaker doubles it after each unsuccessful view, up to a bounded cap, and resets it after a decision.

Run tests:

```bash
go test ./...
go test -race ./...
go vet ./...
```

## Basic HotStuff model

The simulator uses the standard BFT resilience model:

- fixed permissioned replica set;
- `n >= 3f + 1` replicas;
- up to `f` Byzantine replicas;
- quorum size `n - f` (`2f + 1` when `n = 3f + 1`);
- round-robin leaders;
- partial synchrony represented by delays, timeouts, and eventual stabilization;
- safety checks remain active during delay/fault periods;
- liveness is expected after the network stabilizes and a correct leader remains active long enough.

Each view has one leader. Replicas move to a later view on decision or timeout. The next leader collects `NEW_VIEW` messages, selects the highest valid `prepareQC`, obtains the corresponding branch, and proposes a node extending that QC's node.

## Explicit Basic HotStuff phases

1. `NEW_VIEW`: replicas send their highest known `prepareQC` and the branch needed to validate it.
2. `PREPARE`: the leader proposes a new node extending `highQC.node`; replicas validate the node and apply `safeNode`.
3. `PRECOMMIT`: the leader forms a compact `prepareQC`; replicas store it and vote.
4. `COMMIT`: the leader forms a compact `precommitQC`; replicas set `lockedQC = precommitQC` and vote.
5. `DECIDE`: the leader forms a compact `commitQC`; replicas execute newly decided commands through the decided node.

## Compact quorum certificates

Votes contain replica-bound partial authenticators. A QC contains one fixed-size aggregate authenticator over:

```text
<phase, view, nodeID>
```

The in-process threshold oracle releases an aggregate only after receiving at least `n-f` unique, valid vote shares for exactly the same tuple. Individual vote shares are **not embedded in the QC**, so the simulator models the paper's linear authenticator complexity: one leader broadcast plus one partial authenticator from each replica per phase.

This is a semantic simulator of threshold cryptography, not a production BLS/RSA threshold-signature implementation. The oracle intentionally centralizes key material inside the simulator process so that the protocol can model unforgeability and compact QCs without external dependencies.

## Authenticated reliable point-to-point network

Every replica receives a sender-bound transport endpoint:

- an endpoint can authenticate messages only as its own replica ID;
- a message claiming another sender is rejected before delivery;
- all protocol fields are covered by the authentication tag;
- broadcast is implemented as one authenticated point-to-point send per replica, including the sender;
- transient drops are retried until delivery or simulator shutdown;
- delayed/stale messages may still arrive, but view/phase checks reject messages that are no longer applicable.

This models authenticated reliable communication between correct replicas. It is still an in-memory simulator, not TCP/TLS networking.

## `safeNode`, locking, and node validation

`SafeNode(node, justifyQC, lockedQC)` returns true when either:

```text
node extends lockedQC.node
OR
justifyQC.view > lockedQC.view
```

Before this rule is evaluated, the proposal is validated against the local tree:

- full SHA-256 node ID is recomputed;
- parent must exist;
- height must equal `parent.height + 1`;
- proposer and view must match the current leader/view;
- ancestry is reconstructed from local parent links;
- transmitted `Ancestors` data is never trusted.

A received node is inserted only after all validation and HotStuff safety checks pass.

## Branch synchronization

`NEW_VIEW` and phase-transition messages carry the relevant validated branch. A leader no longer falls back to genesis when `highQC.node` is missing. It imports and validates the supplied branch; if the referenced node is still unavailable, it does not propose in that view.

## Pacemaker

The educational Pacemaker uses:

- deterministic round-robin leader selection;
- one timer per view;
- exponential timeout backoff after unsuccessful views;
- a maximum timeout cap;
- timeout reset after a valid decision.

This is sufficient to model the paper's standard eventual-overlap liveness argument more faithfully than a fixed timeout.

## Adversarial scenarios

### `byzantine-equivocation`

A Byzantine leader sends conflicting `PREPARE` proposals to different replicas. Correct replicas enforce one vote per phase/view, so the conflicting proposals cannot both obtain a QC.

### `byzantine-forged-qc`

A Byzantine leader broadcasts a fabricated compact `prepareQC` without quorum vote shares. Correct replicas reject it, timeout, move to a later view, and decide under a correct leader.

Additional tests cover:

- forged aggregate QCs;
- duplicate and mismatched votes;
- vote signer/tuple binding;
- sender spoofing at the transport layer;
- stale phase QCs;
- forged ancestry and node tampering;
- missing `highQC.node` without genesis fallback;
- transient network drops and retry;
- exponential Pacemaker backoff.

## Remaining limitations

- The threshold-signature oracle is educational, not production cryptography.
- No persistent storage or crash recovery.
- No real TCP/TLS peer networking, peer discovery, mempool, batching, or client request deduplication.
- No Chained HotStuff pipelining; this repository intentionally exposes Basic HotStuff phases.
- No production-grade clock synchronization or distributed Pacemaker service.
- Faults remain scenario-driven so traces and tests are deterministic.

## Repository structure

```text
cmd/hotstuff-sim/main.go
internal/hotstuff/crypto.go
internal/hotstuff/node.go
internal/hotstuff/message.go
internal/hotstuff/qc.go
internal/hotstuff/replica.go
internal/hotstuff/protocol.go
internal/hotstuff/safenode.go
internal/banking/command.go
internal/banking/state.go
internal/network/network.go
internal/scenario/scenario.go
internal/scenario/byzantine_forged_qc.go
docs/PROTOCOL.md
```
