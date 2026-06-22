package hotstuff

import (
	"crypto/sha256"
	"encoding/hex"
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
	Ancestors []string
}

func NewGenesisNode() *Node {
	return &Node{ID: GenesisID, Height: 0, Ancestors: nil}
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
	cmdID := "nil"
	cmdText := "nil"
	if command != nil {
		cmdID = command.ID()
		cmdText = command.String()
	}
	raw := fmt.Sprintf("parent=%s|cmd=%s|text=%s|view=%d|leader=%s|height=%d", parentID, cmdID, cmdText, view, proposer, height)
	sum := sha256.Sum256([]byte(raw))
	return &Node{ID: shortDigest(sum[:]), ParentID: parentID, Command: command, Height: height, Proposer: proposer, View: view, Ancestors: ancestors}
}

func shortDigest(b []byte) string { return hex.EncodeToString(b)[:12] }

func (n *Node) Extends(ancestorID string) bool {
	if n == nil || ancestorID == "" {
		return false
	}
	if n.ID == ancestorID {
		return true
	}
	for _, a := range n.Ancestors {
		if a == ancestorID {
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
	return fmt.Sprintf("%s[h=%d view=%d cmd=%s]", n.ID, n.Height, n.View, cmd)
}

type Tree struct {
	Nodes map[string]*Node
}

func NewTree() *Tree {
	g := NewGenesisNode()
	return &Tree{Nodes: map[string]*Node{g.ID: g}}
}

func (t *Tree) Genesis() *Node { return t.Nodes[GenesisID] }

func (t *Tree) Add(node *Node) {
	if node == nil {
		return
	}
	if t.Nodes == nil {
		t.Nodes = make(map[string]*Node)
	}
	t.Nodes[node.ID] = node
}

func (t *Tree) Get(id string) *Node {
	if t == nil {
		return nil
	}
	return t.Nodes[id]
}

func (t *Tree) Extends(nodeID, ancestorID string) bool {
	n := t.Get(nodeID)
	return n != nil && n.Extends(ancestorID)
}

func (t *Tree) Conflicts(a, b string) bool {
	if a == "" || b == "" || a == b {
		return false
	}
	return !t.Extends(a, b) && !t.Extends(b, a)
}

func (t *Tree) BranchTo(nodeID string) []*Node {
	n := t.Get(nodeID)
	if n == nil {
		return nil
	}
	ids := append(append([]string{}, n.Ancestors...), n.ID)
	branch := make([]*Node, 0, len(ids))
	for _, id := range ids {
		if node := t.Get(id); node != nil {
			branch = append(branch, node)
		}
	}
	return branch
}
