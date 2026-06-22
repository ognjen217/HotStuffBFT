package hotstuff

import "fmt"

type Message struct {
	Type    MessageType
	From    string
	To      string
	View    int
	Node    *Node
	Justify *QC
	Vote    *Vote
	Note    string
}

func (m Message) String() string {
	nodeID := "-"
	if m.Node != nil {
		nodeID = m.Node.ID
	}
	qc := "-"
	if m.Justify != nil {
		qc = m.Justify.Short()
	}
	vote := "-"
	if m.Vote != nil {
		vote = fmt.Sprintf("%s/%s/%d/%s", m.Vote.VoterID, m.Vote.Phase, m.Vote.View, m.Vote.NodeID)
	}
	return fmt.Sprintf("%s from=%s to=%s view=%d node=%s qc=%s vote=%s", m.Type, m.From, m.To, m.View, nodeID, qc, vote)
}
