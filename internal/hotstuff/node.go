package hotstuff

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

const GenesisID = "genesis"

type Node struct {
	ID        string
	ParentID  string
	Command   Command
	Height    int
	Proposer  string
	View      int
	Ancestors []string // derived locally; never trusted from the network
}

func NewGenesisNode() *Node {
	return &Node{ID: GenesisID, Height: 0, View: 0, Ancestors: nil}
}

func NewNode(parent *Node, command Command, proposer string, view int) *Node {
	parentID := GenesisID
	height := 1
	ancestors := []string{GenesisID}
	if parent != nil {
		parentID = parent.ID
		height = parent.Height + 1
		ancestors = append(append([]string{}, parent.Ancestors...), parent.ID)
	}
	n := &Node{
		ParentID:  parentID,
		Command:   command,
		Height:    height,
		Proposer:  proposer,
		View:      view,
		Ancestors: ancestors,
	}
	n.ID = n.ExpectedID()
	return n
}

func (n *Node) commandID() string {
	if n == nil || n.Command == nil {
		return "nil"
	}
	return n.Command.ID()
}

func (n *Node) commandText() string {
	if n == nil || n.Command == nil {
		return "nil"
	}
	return n.Command.String()
}

func (n *Node) Canonical() string {
	if n == nil {
		return "<nil-node>"
	}
	return fmt.Sprintf("parent=%s|cmd=%s|text=%s|view=%d|leader=%s|height=%d", n.ParentID, n.commandID(), n.commandText(), n.View, n.Proposer, n.Height)
}

func (n *Node) ExpectedID() string {
	if n == nil {
		return ""
	}
	if n.ID == GenesisID && n.Height == 0 {
		return GenesisID
	}
	sum := sha256.Sum256([]byte(n.Canonical()))
	return hex.EncodeToString(sum[:])
}

func (n *Node) ValidateID() bool {
	if n == nil {
		return false
	}
	if n.ID == GenesisID {
		return n.Height == 0 && n.ParentID == "" && n.Command == nil
	}
	return n.ID == n.ExpectedID()
}

func (n *Node) Clone() *Node {
	if n == nil {
		return nil
	}
	clone := *n
	clone.Ancestors = append([]string{}, n.Ancestors...)
	return &clone
}

func (n *Node) Extends(ancestorID string) bool {
	if n == nil || ancestorID == "" {
		return false
	}
	if n.ID == ancestorID {
		return true
	}
	for _, ancestor := range n.Ancestors {
		if ancestor == ancestorID {
			return true
		}
	}
	return false
}

func (n *Node) String() string {
	if n == nil {
		return "<nil>"
	}
	cmd := "GENESIS"
	if n.Command != nil {
		cmd = n.Command.String()
	}
	shortID := n.ID
	if len(shortID) > 12 {
		shortID = shortID[:12]
	}
	return fmt.Sprintf("%s[h=%d view=%d cmd=%s]", shortID, n.Height, n.View, cmd)
}

type Tree struct {
	Nodes map[string]*Node
}

func NewTree() *Tree {
	genesis := NewGenesisNode()
	return &Tree{Nodes: map[string]*Node{genesis.ID: genesis}}
}

func (t *Tree) Genesis() *Node {
	if t == nil {
		return nil
	}
	return t.Nodes[GenesisID]
}

// PrepareNode validates a node against the locally known parent and reconstructs
// the derived ancestry. It does not mutate the tree.
func (t *Tree) PrepareNode(node *Node) (*Node, error) {
	if t == nil || node == nil {
		return nil, errors.New("nil tree or node")
	}
	if node.ID == GenesisID {
		if !node.ValidateID() {
			return nil, errors.New("invalid genesis node")
		}
		return NewGenesisNode(), nil
	}
	if !node.ValidateID() {
		return nil, fmt.Errorf("node hash mismatch for %q", node.ID)
	}
	parent := t.Get(node.ParentID)
	if parent == nil {
		return nil, fmt.Errorf("missing parent %q for node %q", node.ParentID, node.ID)
	}
	if node.Height != parent.Height+1 {
		return nil, fmt.Errorf("invalid height for node %q: got %d want %d", node.ID, node.Height, parent.Height+1)
	}
	prepared := node.Clone()
	prepared.Ancestors = append(append([]string{}, parent.Ancestors...), parent.ID)
	return prepared, nil
}

func (t *Tree) AddValidated(node *Node) (*Node, error) {
	prepared, err := t.PrepareNode(node)
	if err != nil {
		return nil, err
	}
	if t.Nodes == nil {
		t.Nodes = make(map[string]*Node)
	}
	t.Nodes[prepared.ID] = prepared
	return prepared, nil
}

// Add is reserved for locally created nodes. Network-originating nodes should be
// passed through PrepareNode and inserted only after the HotStuff safety checks.
func (t *Tree) Add(node *Node) {
	_, _ = t.AddValidated(node)
}

func (t *Tree) ImportBranch(branch []*Node) error {
	if t == nil {
		return errors.New("nil tree")
	}
	for _, node := range branch {
		if node == nil {
			return errors.New("branch contains nil node")
		}
		if node.ID == GenesisID {
			continue
		}
		if _, exists := t.Nodes[node.ID]; exists {
			continue
		}
		if _, err := t.AddValidated(node); err != nil {
			return err
		}
	}
	return nil
}

func (t *Tree) Get(id string) *Node {
	if t == nil {
		return nil
	}
	return t.Nodes[id]
}

// Extends follows locally stored parent links instead of trusting a transmitted
// ancestor list.
func (t *Tree) Extends(nodeID, ancestorID string) bool {
	if t == nil || nodeID == "" || ancestorID == "" {
		return false
	}
	current := t.Get(nodeID)
	for current != nil {
		if current.ID == ancestorID {
			return true
		}
		if current.ID == GenesisID {
			break
		}
		current = t.Get(current.ParentID)
	}
	return false
}

func (t *Tree) Conflicts(a, b string) bool {
	if a == "" || b == "" || a == b {
		return false
	}
	return !t.Extends(a, b) && !t.Extends(b, a)
}

func (t *Tree) BranchTo(nodeID string) []*Node {
	node := t.Get(nodeID)
	if node == nil {
		return nil
	}
	ids := append(append([]string{}, node.Ancestors...), node.ID)
	branch := make([]*Node, 0, len(ids))
	for _, id := range ids {
		if stored := t.Get(id); stored != nil {
			branch = append(branch, stored.Clone())
		}
	}
	return branch
}
