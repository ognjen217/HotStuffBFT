package hotstuff

import (
	"errors"
	"fmt"
)

type Vote struct {
	VoterID          string
	Phase            Phase
	View             int
	NodeID           string
	PartialSignature string
}

type QC struct {
	Phase              Phase
	View               int
	NodeID             string
	AggregateSignature string
	Genesis            bool
}

func GenesisQC() *QC {
	return &QC{Phase: PhaseGenesis, View: 0, NodeID: GenesisID, Genesis: true}
}

func NewQC(phase Phase, view int, nodeID string, votes []Vote, quorum int, crypto *ReplicaCrypto) (*QC, error) {
	if crypto == nil {
		return nil, errors.New("threshold crypto is required")
	}
	aggregate, err := crypto.Combine(phase, view, nodeID, votes, quorum)
	if err != nil {
		return nil, err
	}
	return &QC{
		Phase:              phase,
		View:               view,
		NodeID:             nodeID,
		AggregateSignature: aggregate,
	}, nil
}

func (qc *QC) Valid(crypto *ReplicaCrypto, quorum int) bool {
	return crypto != nil && crypto.VerifyQC(qc, quorum)
}

func (qc *QC) Clone() *QC {
	if qc == nil {
		return nil
	}
	copyQC := *qc
	return &copyQC
}

func (qc *QC) Short() string {
	if qc == nil {
		return "<nil QC>"
	}
	if qc.Genesis {
		return "genesisQC"
	}
	sig := qc.AggregateSignature
	if len(sig) > 12 {
		sig = sig[:12]
	}
	return fmt.Sprintf("%sQC(node=%s view=%d aggregate=%s)", qc.Phase, qc.NodeID, qc.View, sig)
}
