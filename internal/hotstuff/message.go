package hotstuff

import (
	"fmt"
	"strings"
)

type Message struct {
	Type    MessageType
	From    string
	To      string
	View    int
	Node    *Node
	Branch  []*Node
	Justify *QC
	Vote    *Vote
	Note    string
	AuthTag string
}

// Canonical returns the authenticated payload. AuthTag itself is intentionally
// excluded. Every field that affects protocol meaning is covered.
func (m Message) Canonical() string {
	var b strings.Builder
	fmt.Fprintf(&b, "type=%s|from=%s|to=%s|view=%d|note=%s", m.Type, m.From, m.To, m.View, m.Note)
	if m.Node == nil {
		b.WriteString("|node=-")
	} else {
		fmt.Fprintf(&b, "|node=%s|node-data=%s", m.Node.ID, m.Node.Canonical())
	}
	if m.Justify == nil {
		b.WriteString("|qc=-")
	} else {
		fmt.Fprintf(&b, "|qc=%s,%d,%s,%s,%t", m.Justify.Phase, m.Justify.View, m.Justify.NodeID, m.Justify.AggregateSignature, m.Justify.Genesis)
	}
	if m.Vote == nil {
		b.WriteString("|vote=-")
	} else {
		fmt.Fprintf(&b, "|vote=%s,%s,%d,%s,%s", m.Vote.VoterID, m.Vote.Phase, m.Vote.View, m.Vote.NodeID, m.Vote.PartialSignature)
	}
	b.WriteString("|branch=")
	for i, node := range m.Branch {
		if i > 0 {
			b.WriteByte(';')
		}
		if node == nil {
			b.WriteString("<nil>")
			continue
		}
		fmt.Fprintf(&b, "%s:%s", node.ID, node.Canonical())
	}
	return b.String()
}

func (m Message) String() string {
	nodeID := "-"
	if m.Node != nil {
		nodeID = m.Node.ID
		if len(nodeID) > 12 {
			nodeID = nodeID[:12]
		}
	}
	qc := "-"
	if m.Justify != nil {
		qc = m.Justify.Short()
	}
	vote := "-"
	if m.Vote != nil {
		vote = fmt.Sprintf("%s/%s/%d/%s", m.Vote.VoterID, m.Vote.Phase, m.Vote.View, m.Vote.NodeID)
	}
	return fmt.Sprintf("%s from=%s to=%s view=%d node=%s qc=%s vote=%s branch=%d", m.Type, m.From, m.To, m.View, nodeID, qc, vote, len(m.Branch))
}
