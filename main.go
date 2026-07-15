package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"sync"
	"time"
)

type Cluster struct {
	Network  *Network
	Nodes    []*Node
	Verifier *ThresholdVerifier
}

func NewCluster(dropRate float64, delayMax int, baseTimeout time.Duration) (*Cluster, error) {
	nodeIDs := []string{"Node-1", "Node-2", "Node-3", "Node-4"}
	f := (len(nodeIDs) - 1) / 3
	quorum := len(nodeIDs) - f
	signers, verifier, err := NewCryptoSuite(nodeIDs, quorum)
	if err != nil {
		return nil, err
	}

	network := NewNetwork(nodeIDs, dropRate, delayMax)
	cluster := &Cluster{Network: network, Verifier: verifier}
	for _, nodeID := range nodeIDs {
		cluster.Nodes = append(cluster.Nodes, NewNode(
			nodeID,
			network,
			len(nodeIDs),
			signers[nodeID],
			verifier,
			baseTimeout,
		))
	}
	for _, node := range cluster.Nodes {
		node.Start()
	}
	return cluster, nil
}

func (c *Cluster) Stop() {
	for _, node := range c.Nodes {
		node.Stop()
	}
}

func (c *Cluster) CrashNode(nodeID string) {
	c.Network.CrashNode(nodeID)
	for _, node := range c.Nodes {
		if node.ID == nodeID {
			node.Stop()
			return
		}
	}
}

func (c *Cluster) WaitForExecution(commandID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allExecuted := true
		for _, node := range c.Nodes {
			if !node.HasExecuted(commandID) {
				allExecuted = false
				break
			}
		}
		if allExecuted {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}

var simulationMu sync.Mutex

func main() {
	useWeb := flag.Bool("web", true, "run the web interface")
	cliScenario := flag.String("scenario", "", "scenario to run from the CLI")
	flag.Parse()

	if *cliScenario != "" {
		runSimulation(*cliScenario)
		return
	}
	if !*useWeb {
		return
	}

	http.Handle("/assets/", http.StripPrefix("/assets/", http.FileServer(http.Dir("assets"))))

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, "index.html")
	})

	http.HandleFunc("/run", func(w http.ResponseWriter, r *http.Request) {
		scenario := r.URL.Query().Get("scenario")
		if scenario == "" {
			scenario = "happy"
		}
		go func() {
			simulationMu.Lock()
			defer simulationMu.Unlock()
			runSimulation(scenario)
		}()
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("started"))
	})

	http.HandleFunc("/logs", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(GetLogs())
	})

	http.HandleFunc("/visual-state", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-store")
		_ = json.NewEncoder(w).Encode(CurrentVisualState())
	})

	fmt.Println("[System] Web server running on http://localhost:8081")
	if err := http.ListenAndServe(":8081", nil); err != nil {
		fmt.Println("[Error] Server failed to start:", err)
	}
}

func runSimulation(scenario string) {
	ClearLogs()
	AddLog("\n=== STARTING SCENARIO: %s ===\n", scenario)

	delayMax := 50
	baseTimeout := 1200 * time.Millisecond
	if scenario == "delayed-network" {
		delayMax = 3000
		baseTimeout = 1500 * time.Millisecond
	}

	cluster, err := NewCluster(0, delayMax, baseTimeout)
	if err != nil {
		AddLog("[System] Cannot create cluster: %v\n", err)
		return
	}
	BeginVisualSimulation(cluster, scenario)
	defer func() {
		cluster.Stop()
		EndVisualSimulation(cluster)
	}()
	time.Sleep(150 * time.Millisecond)

	switch scenario {
	case "happy", "banking-block-transfer":
		runBankingScenario(cluster)
	case "silent-leader":
		runSilentLeaderScenario(cluster)
	case "byzantine-equivocation":
		runEquivocationScenario(cluster)
	case "delayed-network":
		runDelayedNetworkScenario(cluster)
	default:
		AddLog("[System] Unknown scenario %q.\n", scenario)
	}

	AddLog("=== END SCENARIO: %s ===\n\n", scenario)
}

func runBankingScenario(cluster *Cluster) {
	commands := []Command{
		{ID: "b128", Type: BlockAccount, From: "Marko", Metadata: "fraud-check"},
		{ID: "b129", Type: Transfer, From: "Marko", To: "Ana", Amount: 500},
		{ID: "b130", Type: ApproveLoan, From: "Ana", Amount: 2000, Metadata: "L-42"},
	}

	for _, cmd := range commands {
		AddLog("[Client] Broadcasting command %s to all replicas.\n", cmd.ID)
		cluster.Network.BroadcastClientCommand(cmd)
		if !cluster.WaitForExecution(cmd.ID, 8*time.Second) {
			AddLog("[System] Command %s was not executed by all replicas before the scenario deadline.\n", cmd.ID)
			return
		}
	}
	AddLog("[System] All replicas executed the same three-command log.\n")
}

func runSilentLeaderScenario(cluster *Cluster) {
	AddLog("[System] Crashing leader Node-1 in view 1.\n")
	cluster.CrashNode("Node-1")
	cmd := Command{ID: "b130", Type: ApproveLoan, From: "Ana", Amount: 2000, Metadata: "L-42"}
	cluster.Network.BroadcastClientCommand(cmd)

	if waitForCorrectReplicas(cluster.Nodes[1:], cmd.ID, 12*time.Second) {
		AddLog("[System] The three correct replicas committed after rotating to a correct leader.\n")
	} else {
		AddLog("[System] Correct replicas did not all commit the command.\n")
	}
}

func runEquivocationScenario(cluster *Cluster) {
	cmdA := Command{ID: "evil-1", Type: Transfer, From: "Marko", To: "Ana", Amount: 500}
	cmdB := Command{ID: "evil-2", Type: Transfer, From: "Marko", To: "Luka", Amount: 500}
	nodeA := &TreeNode{ParentHash: GenesisHash, Cmd: cmdA, ProposedView: 1}
	nodeA.Hash = hashNode(nodeA)
	nodeB := &TreeNode{ParentHash: GenesisHash, Cmd: cmdB, ProposedView: 1}
	nodeB.Hash = hashNode(nodeB)
	cluster.Network.StoreNode(nodeA)
	cluster.Network.StoreNode(nodeB)

	AddLog("[System] Byzantine leader equivocates in view 1 and deliberately withholds its own vote.\n")
	cluster.Network.Send("Node-1", "Node-2", Message{Type: Prepare, ViewNumber: 1, Node: nodeA})
	cluster.Network.Send("Node-1", "Node-3", Message{Type: Prepare, ViewNumber: 1, Node: nodeB})
	cluster.Network.Send("Node-1", "Node-4", Message{Type: Prepare, ViewNumber: 1, Node: nodeB})
	time.Sleep(250 * time.Millisecond)
	cluster.CrashNode("Node-1")

	recovery := Command{ID: "b130", Type: ApproveLoan, From: "Ana", Amount: 2000, Metadata: "L-42"}
	cluster.Network.BroadcastClientCommand(recovery)
	if waitForCorrectReplicas(cluster.Nodes[1:], recovery.ID, 12*time.Second) {
		AddLog("[System] No two conflicting QCs formed; the next correct leader committed a safe proposal.\n")
	} else {
		AddLog("[System] Recovery command was not committed by all correct replicas.\n")
	}
}

func runDelayedNetworkScenario(cluster *Cluster) {
	cmd := Command{ID: "b128", Type: BlockAccount, From: "Marko", Metadata: "fraud-check"}
	AddLog("[System] Message delay is up to 3 seconds; timeout backoff may cause several view changes.\n")
	cluster.Network.BroadcastClientCommand(cmd)
	if cluster.WaitForExecution(cmd.ID, 45*time.Second) {
		AddLog("[System] Command committed after the pacemaker obtained a sufficiently long stable view.\n")
	} else {
		AddLog("[System] Command was not executed by all replicas within the scenario deadline.\n")
	}
}

func waitForCorrectReplicas(nodes []*Node, commandID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		allExecuted := true
		for _, node := range nodes {
			if !node.HasExecuted(commandID) {
				allExecuted = false
				break
			}
		}
		if allExecuted {
			return true
		}
		time.Sleep(20 * time.Millisecond)
	}
	return false
}
