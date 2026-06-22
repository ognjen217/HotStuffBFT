# HotStuffBFT

Educational Go simulator for **Basic HotStuff BFT consensus** using a concrete permissioned banking-network case study.

This is not a production blockchain. It is a clear, testable simulator for learning how replicas use views, leaders, QCs, locks, and the Basic HotStuff phases to agree on a deterministic ledger order even when one replica is faulty.

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

The important behavior is deterministic execution after consensus. If `b128` is decided before `b129`, then Marko is already blocked, so the transfer is included in the decided ledger but is marked invalid/rejected by every correct replica. Consensus agrees on the order; the state machine deterministically decides whether each command is valid.

## Run

```bash
go run ./cmd/hotstuff-sim --scenario happy
go run ./cmd/hotstuff-sim --scenario silent-leader
go run ./cmd/hotstuff-sim --scenario byzantine-equivocation
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

By default, each run now saves the terminal output to `logs/<scenario>.txt` and calls the Python visualizer to create `visualizations/<scenario>.html`:

```bash
go run ./cmd/hotstuff-sim --scenario byzantine-equivocation --timeout-ms 1000
xdg-open visualizations/byzantine-equivocation.html
```

You can also call the visualizer manually:

```bash
python3 scripts/visualize_log.py --log logs/happy.txt --out visualizations/happy.html
```

See `docs/VISUALIZATION.md` for details.

Run tests:

```bash
go test ./...
```

## Basic HotStuff model used here

The simulator uses the standard BFT resilience model:

- `n = 3f + 1` replicas
- up to `f` Byzantine replicas
- quorum size `n - f = 2f + 1`
- default: `n=4`, `f=1`, quorum `3`
- leader-based views with round-robin leader selection
- partial synchrony simulated with message delays and timeouts

Each view has one leader. Replicas move to a later view on decision or timeout. The next leader collects `NEW_VIEW` messages, selects the highest known `prepareQC`, and proposes a node extending that QC's node.

## Explicit Basic HotStuff phases

The code models the five requested phases directly:

1. `NEW_VIEW`: replicas send their highest known `prepareQC` to the next leader.
2. `PREPARE`: leader proposes a new node extending `highQC.node`; replicas check `safeNode` and vote.
3. `PRECOMMIT`: leader forms `prepareQC`; replicas store it and vote.
4. `COMMIT`: leader forms `precommitQC`; replicas set `lockedQC = precommitQC` and vote.
5. `DECIDE`: leader forms `commitQC`; replicas execute newly decided commands through the decided node.

## Quorum certificates

A `QC` is simulated by collecting unique voter IDs. A QC is valid only if:

- it has at least quorum unique voters,
- every vote has the same phase,
- every vote has the same view,
- every vote has the same node ID.

**This simulator models QC semantics but does not implement production cryptography.** There are no real threshold signatures, no real private keys, and no network authentication layer.

## `lockedQC`

A replica updates `lockedQC` during the `COMMIT` phase when it receives a valid `precommitQC`. The lock prevents the replica from later voting for a conflicting branch unless a later-view QC justifies the proposal. This is the core safety mechanism that stops two conflicting branches from both being decided by correct replicas.

## `safeNode`

`SafeNode(node, justifyQC, lockedQC)` returns true if either:

```text
node extends lockedQC.node
OR
justifyQC.view > lockedQC.view
```

Genesis or empty locks are treated as safe. The function is implemented separately in `internal/hotstuff/safenode.go` and tested directly.

## Scenarios

### `happy`

All replicas are correct. The banking commands are proposed and decided in order. All correct replicas finish with the same ledger and same banking state.

### `silent-leader`

The first leader receives `NEW_VIEW` messages but sends no proposal. Replicas timeout, move to the next view, and the next leader continues normally using the highest known QC.

### `byzantine-equivocation`

Replica `B1` is a Byzantine leader in view 1. It sends conflicting `PREPARE` proposals:

- primary: `b128: BLOCK_ACCOUNT(Marko)`
- conflict: `b128-prime: TRANSFER(Marko, Luka, 500)`

Correct replicas obey the one-vote-per-phase-per-view rule, so a conflicting proposal cannot collect a valid QC from correct replicas. The trace shows accepted and rejected votes, then a timeout/view-change recovery.

### `banking-block-transfer`

The default banking example is emphasized: Marko is blocked first, and the later transfer from Marko is deterministically rejected by all correct replicas.

### `delayed-network`

Early messages are delayed beyond the timeout to mimic a pre-GST period. After the network stabilizes, a correct leader decides.

## What is simplified compared to production HotStuff

- No real threshold signatures or cryptographic authentication.
- No persistent storage or crash recovery.
- No TCP, TLS, peer discovery, mempool, batching, or client request deduplication.
- No Chained HotStuff pipelining; this is Basic HotStuff with explicit phases.
- No production pacemaker; timeouts are simple educational timers.
- Faults are scenario-driven and deterministic so the trace is readable and tests are stable.

## Repository structure

```text
cmd/hotstuff-sim/main.go
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
internal/scenario/happy.go
internal/scenario/silent_leader.go
internal/scenario/byzantine_equivocation.go
internal/scenario/banking_block_transfer.go
internal/scenario/delayed_network.go
docs/PROTOCOL.md
README.md
```
