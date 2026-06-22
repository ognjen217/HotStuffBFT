package hotstuff

import (
	"errors"
	"fmt"
	"sort"
)

type Vote struct {
	VoterID string
	Phase   Phase
	View    int
	NodeID  string
}

type QC struct {
	Phase   Phase
	View    int
	NodeID  string
	Voters  []string
	Votes   []Vote
	Quorum  int
	Genesis bool
}

func GenesisQC() *QC {
	return &QC{Phase: PhaseGenesis, View: 0, NodeID: GenesisID, Quorum: 0, Genesis: true}
}

func NewQC(phase Phase, view int, nodeID string, votes []Vote, quorum int) (*QC, error) {
	if quorum <= 0 {
		return nil, errors.New("quorum must be positive")
	}
	seen := make(map[string]Vote)
	for _, vote := range votes {
		if vote.VoterID == "" {
			return nil, errors.New("vote with empty voter id")
		}
		if vote.Phase != phase || vote.View != view || vote.NodeID != nodeID {
			return nil, fmt.Errorf("vote mismatch: got (%s,%d,%s), want (%s,%d,%s)", vote.Phase, vote.View, vote.NodeID, phase, view, nodeID)
		}
		seen[vote.VoterID] = vote
	}
	if len(seen) < quorum {
		return nil, fmt.Errorf("not enough unique voters: got %d need %d", len(seen), quorum)
	}
	voters := make([]string, 0, len(seen))
	uniqVotes := make([]Vote, 0, len(seen))
	for voter, vote := range seen {
		voters = append(voters, voter)
		uniqVotes = append(uniqVotes, vote)
	}
	sort.Strings(voters)
	sort.Slice(uniqVotes, func(i, j int) bool { return uniqVotes[i].VoterID < uniqVotes[j].VoterID })
	return &QC{Phase: phase, View: view, NodeID: nodeID, Voters: voters, Votes: uniqVotes, Quorum: quorum}, nil
}

func (qc *QC) Valid() bool {
	if qc == nil {
		return false
	}
	if qc.Genesis {
		return qc.NodeID == GenesisID && qc.View == 0
	}
	if qc.Quorum <= 0 || len(qc.Voters) < qc.Quorum || len(qc.Votes) < qc.Quorum {
		return false
	}
	seen := make(map[string]struct{})
	for _, vote := range qc.Votes {
		if vote.Phase != qc.Phase || vote.View != qc.View || vote.NodeID != qc.NodeID || vote.VoterID == "" {
			return false
		}
		seen[vote.VoterID] = struct{}{}
	}
	if len(seen) < qc.Quorum {
		return false
	}
	for _, voter := range qc.Voters {
		if _, ok := seen[voter]; !ok {
			return false
		}
	}
	return true
}

func (qc *QC) Clone() *QC {
	if qc == nil {
		return nil
	}
	copyQC := *qc
	copyQC.Voters = append([]string{}, qc.Voters...)
	copyQC.Votes = append([]Vote{}, qc.Votes...)
	return &copyQC
}

func (qc *QC) Short() string {
	if qc == nil {
		return "<nil QC>"
	}
	if qc.Genesis {
		return "genesisQC"
	}
	return fmt.Sprintf("%sQC(node=%s view=%d voters=%v)", qc.Phase, qc.NodeID, qc.View, qc.Voters)
}
