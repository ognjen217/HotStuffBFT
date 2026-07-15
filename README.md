# Basic HotStuff BFT Simulator

A small educational simulator of the **Basic HotStuff** Byzantine Fault Tolerant consensus protocol described in Algorithm 2 of the HotStuff paper.

The project runs four replicas, simulates authenticated point-to-point communication, executes banking commands through a replicated state machine, and displays the protocol as a live Mermaid sequence diagram alongside textual logs.

> This project implements the non-pipelined **Basic HotStuff** flow: `NEW-VIEW -> PREPARE -> PRE-COMMIT -> COMMIT -> DECIDE`. It is not an implementation of Chained HotStuff and it is not intended for production deployment.

## What the project demonstrates

- a fixed cluster with `n = 4` replicas and tolerance of `f = 1` Byzantine fault;
- deterministic round-robin leader selection;
- quorum size `n - f = 2f + 1 = 3`;
- collection of `NEW-VIEW` messages and selection of the highest `prepareQC`;
- the HotStuff safety predicate based on ancestry and `lockedQC`;
- one vote per replica, phase, and view;
- formation and verification of quorum certificates;
- locking during the commit phase;
- execution of the committed branch in identical order on every correct replica;
- timeout-driven view changes with exponential backoff;
- live protocol visualization in the browser.

## Project structure

```text
.
├── assets/
│   └── mermaid.min.js       Local Mermaid library used by the browser
├── crypto.go                Signature shares and idealized threshold-QC verification
├── domain.go                Shared protocol, command, node, QC, and message types
├── go.mod                   Go module definition
├── index.html               Minimal web interface and live sequence diagram
├── logger.go                Thread-safe simulation log storage
├── main.go                  Cluster creation, HTTP server, and scenario orchestration
├── network.go               Simulated authenticated network and shared node retrieval
├── node.go                  Core Basic HotStuff replica and leader logic
├── pacemaker.go             View timeout and exponential-backoff logic
├── state_machine.go         Replicated banking application state machine
├── visualization.go         Read-only state and event model for the web interface
├── visualization_test.go    Visualization-state consistency test
└── CODE_RUNDOWN.md          Detailed explanation of every project file
```

A more detailed file-by-file explanation is available in [`CODE_RUNDOWN.md`](CODE_RUNDOWN.md).

## Requirements

- Go 1.23 or newer;
- a modern web browser;
- no external JavaScript installation and no internet connection are required for Mermaid.

Check the installed Go version:

```bash
go version
```

## Running the web interface

From the project directory:

```bash
go run .
```

Open the following address in a browser:

```text
http://localhost:8081
```

The page contains:

1. buttons for selecting a simulation scenario;
2. a live Mermaid message sequence diagram;
3. the complete textual protocol log.

Only one scenario is executed at a time. The browser polls the server every 350 ms and updates both the sequence diagram and log output.

## Running a scenario from the command line

The same scenarios can be executed without the browser:

```bash
go run . -scenario happy
go run . -scenario silent-leader
go run . -scenario byzantine-equivocation
go run . -scenario banking-block-transfer
go run . -scenario delayed-network
```

To disable the web server without selecting a CLI scenario:

```bash
go run . -web=false
```

## Available scenarios

### Happy Path

Runs the normal protocol with four responsive replicas. Three banking commands are submitted one at a time and every replica commits and executes the same ordered log.

### Banking Commands

Uses the same three-command sequence as the happy-path scenario to emphasize replicated state-machine behavior:

1. block Marko's account;
2. attempt a transfer from the blocked account;
3. approve a loan for Ana.

The consensus protocol orders commands; the banking state machine independently determines whether each ordered command is valid.

### Silent Leader

Crashes `Node-1`, which is the leader of view 1. The remaining replicas time out, enter a later view, send `NEW-VIEW` messages to the next leader, and commit the pending command once a correct leader has a sufficiently long view.

### Byzantine Equivocation

The first leader sends conflicting proposals to different replicas and withholds its own vote. Correct replicas obey the one-vote-per-phase-and-view rule, so two conflicting quorum certificates cannot be formed. After the faulty leader is removed, a later correct leader commits a safe recovery command.

### Delayed Network

Introduces random delays of up to three seconds. The pacemaker may trigger several view changes, and its timeout grows exponentially until the system obtains a sufficiently long stable view.

## End-to-end execution flow

The following sequence explains what happens from the moment a user starts a scenario until a command is executed.

### 1. Scenario selection

The browser calls:

```text
GET /run?scenario=<scenario-name>
```

`main.go` creates a fresh cluster, clears previous logs, registers the active visualization state, and starts four replicas.

### 2. Cluster initialization

`NewCluster`:

1. defines `Node-1` through `Node-4`;
2. calculates `f = 1` and quorum `3`;
3. creates an Ed25519 key pair for each replica;
4. creates the simulated network;
5. constructs and starts every node and its pacemaker.

Each replica begins in view 1 and sends a `NEW-VIEW` message to the deterministic leader of that view.

### 3. Client command distribution

The scenario sends a banking `Command` to every non-crashed replica through `BroadcastClientCommand`.

Each replica stores the command in its local pending queue. Only the current leader can turn a pending command into a proposal.

### 4. NEW-VIEW and leader proposal

The leader waits until it has `n - f` unique `NEW-VIEW` messages. It selects the highest valid `prepareQC` carried by those messages as `highQC`.

The leader then:

1. chooses the next pending command;
2. creates a new `TreeNode` whose parent is `highQC.node`, or `GENESIS` when no QC exists;
3. hashes the complete parent-and-command representation;
4. broadcasts a `PREPARE` proposal.

### 5. PREPARE

A replica accepts the proposal only when:

- it came from the leader of the current view;
- the node hash is correct;
- the proposal extends the node justified by the attached QC;
- the QC is valid;
- the HotStuff `safeNode` predicate is satisfied.

The `safeNode` predicate accepts a proposal when either:

- the proposal extends the replica's locked branch; or
- the justification QC has a higher view than the replica's current lock.

An accepting replica signs the tuple:

```text
<PREPARE, viewNumber, nodeHash>
```

and sends one vote to the leader.

### 6. PRE-COMMIT

After collecting three valid and unique prepare votes for the same node and view, the leader combines them into a `prepareQC` and broadcasts `PRE-COMMIT`.

Replicas verify the QC, remember it as their highest `PrepareQC`, and send a pre-commit vote.

### 7. COMMIT and locking

After collecting a quorum of pre-commit votes, the leader forms a `precommitQC` and broadcasts `COMMIT`.

Each accepting replica updates:

```text
LockedQC = precommitQC
```

and sends a commit vote. This lock is the central safety mechanism that prevents conflicting committed branches.

### 8. DECIDE and state-machine execution

After collecting a quorum of commit votes, the leader creates a `commitQC` and broadcasts `DECIDE`.

Every replica:

1. verifies the `commitQC`;
2. reconstructs the branch from its last executed node to the committed node;
3. executes all previously unexecuted commands in parent-to-child order;
4. records the commands as executed;
5. advances to the next view.

### 9. View change and pacemaker

If a replica does not decide before its timer expires, the pacemaker issues a `nextView` interrupt. The replica advances its view and sends its highest `PrepareQC` to the next leader.

The timeout doubles after unsuccessful views and is reset to the base timeout after a successful decision.

### 10. Browser updates

The browser repeatedly requests:

```text
GET /visual-state
GET /logs
```

`visualization.go` converts snapshots of the cluster and recorded transport events into JSON. `index.html` converts recent events into Mermaid sequence-diagram syntax and renders the resulting SVG.

## HTTP endpoints

| Endpoint                     | Purpose                                                    |
| ---------------------------- | ---------------------------------------------------------- |
| `GET /`                      | Serves the web interface                                   |
| `GET /assets/mermaid.min.js` | Serves the local Mermaid bundle                            |
| `GET /run?scenario=<name>`   | Starts a scenario and returns HTTP 202                     |
| `GET /logs`                  | Returns the current log as a JSON array of strings         |
| `GET /visual-state`          | Returns a read-only JSON snapshot used by the live diagram |

## Replicated banking state machine

The consensus layer orders commands; `state_machine.go` applies them deterministically.

Supported command types:

| Command         | Effect                                                                |
| --------------- | --------------------------------------------------------------------- |
| `TRANSFER`      | Moves funds when the sender is not blocked and has sufficient balance |
| `BLOCK_ACCOUNT` | Marks an account as blocked                                           |
| `APPROVE_LOAN`  | Records a loan and credits the specified account                      |

Initial balances:

| Account | Balance |
| ------- | ------: |
| Marko   |    1000 |
| Ana     |     200 |
| Luka    |     100 |

A command can be committed by consensus but rejected by the application state machine. For example, a transfer from a blocked account is still part of the ordered log, but applying it produces no balance change. Every correct replica reaches the same result because execution is deterministic.

## Cryptographic model

The paper assumes a threshold-signature primitive with operations equivalent to `tsign`, `tcombine`, and `tverify`.

This simulator models that interface as follows:

- every replica owns a real Ed25519 key pair;
- votes are real Ed25519 signatures over `<phase, view, nodeHash>`;
- the verifier checks all shares before forming a QC;
- a QC carries one opaque combined token;
- the verified shares behind that token are retained in the simulator's in-memory verifier.

This preserves the protocol behavior required for the simulation, including identity, message binding, uniqueness, and QC validation. It is **not** a distributed production threshold-signature implementation such as BLS threshold signatures.

## Safety-related checks implemented by the simulator

- the transport stamps the sender identity instead of trusting a message-provided ID;
- unknown replica identities are rejected;
- stale messages are ignored and future-view messages are buffered;
- only the deterministic leader may send phase-control messages;
- a replica votes at most once per phase and view;
- vote sets are indexed by phase, view, and node hash;
- signature shares from different tuples cannot be combined;
- a QC must match the expected phase, view, and node;
- a proposal must extend its justification node;
- a `DECIDE` message without a valid `commitQC` cannot execute a command;
- node hashes cover the complete command, including transfer recipient;
- committed commands are executed at most once.

## Tests and static analysis

Run the normal test suite:

```bash
go test ./...
```

Run tests with Go's race detector:

```bash
go test -race ./...
```

Run static analysis:

```bash
go vet ./...
```

The tests cover:

- complete command hashing;
- rejection of signature shares from mixed views;
- one vote per phase and view;
- rejection of forged `DECIDE` messages;
- identical happy-path execution on all four replicas;
- JSON-serializable and protocol-consistent visualization state.

## How to extend the project

### Add a new banking command

1. Add a new value to `CommandType` in `domain.go`.
2. Add its validation and deterministic behavior to `StateMachine.Execute`.
3. Create commands of that type in a scenario in `main.go`.
4. Add unit tests for valid, invalid, and repeated execution cases.

### Add a new simulation scenario

1. Add a new scenario function in `main.go`.
2. Add the scenario name to the `runSimulation` switch.
3. Add a matching button in `index.html`.
4. Use `Network.Send`, `BroadcastClientCommand`, `CrashNode`, or configurable delays to model the behavior.
5. Add a test when the scenario introduces new protocol behavior.

### Change cluster size

The current scenarios and UI assume four replicas. To experiment with another valid HotStuff cluster size:

1. change the `nodeIDs` list in `NewCluster`;
2. preserve `n >= 3f + 1`;
3. ensure the intended fault count and quorum are still calculated correctly;
4. update scenarios that refer to specific node IDs.

## Important limitations

- This is an in-process simulator; replicas are goroutines, not separate machines.
- The shared `NodeStore` simplifies retrieval of missing ancestors.
- The threshold certificate implementation is idealized and in-memory.
- Network authentication is modeled by the trusted simulated transport.
- Client reply collection, request numbering, persistence, recovery, storage durability, and production networking are outside the scope of the project.
- The visual endpoint exposes more information than the minimal web page currently displays.
- The implementation follows Basic HotStuff rather than Chained HotStuff or a pipelined production implementation.

## Recommended reading order

For understanding the source code, read the files in this order:

1. `domain.go`
2. `main.go`
3. `network.go`
4. `crypto.go`
5. `node.go`
6. `pacemaker.go`
7. `state_machine.go`
8. `visualization.go`
9. `index.html`
10. the test files
