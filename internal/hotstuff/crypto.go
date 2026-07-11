package hotstuff

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// SimulatedThresholdOracle models the semantics of a (2f+1,n) threshold
// signature scheme while keeping quorum certificates compact. It is intentionally
// an in-process educational oracle, not production cryptography: only Combine can
// mint a fixed-size aggregate authenticator, and Combine succeeds only after it
// receives a quorum of valid, unique partial signatures.
type SimulatedThresholdOracle struct {
	mu         sync.RWMutex
	memberKeys map[string][]byte
	aggregates map[string]aggregateRecord
}

type aggregateRecord struct {
	Phase  Phase
	View   int
	NodeID string
	Quorum int
}

// ReplicaCrypto is the capability given to one replica. It can sign only as the
// bound replica, but it can verify any member's vote/message and combine a quorum
// of valid vote shares.
type ReplicaCrypto struct {
	selfID string
	oracle *SimulatedThresholdOracle
}

func NewSimulatedThresholdOracle(replicaIDs []string, seed string) *SimulatedThresholdOracle {
	if seed == "" {
		seed = "hotstuff-educational-threshold-oracle"
	}
	keys := make(map[string][]byte, len(replicaIDs))
	for _, id := range replicaIDs {
		sum := sha256.Sum256([]byte(seed + "|member|" + id))
		keys[id] = append([]byte(nil), sum[:]...)
	}
	return &SimulatedThresholdOracle{
		memberKeys: keys,
		aggregates: make(map[string]aggregateRecord),
	}
}

func (o *SimulatedThresholdOracle) ForReplica(id string) (*ReplicaCrypto, error) {
	if o == nil {
		return nil, errors.New("nil threshold oracle")
	}
	o.mu.RLock()
	_, ok := o.memberKeys[id]
	o.mu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("unknown replica %q", id)
	}
	return &ReplicaCrypto{selfID: id, oracle: o}, nil
}

func (c *ReplicaCrypto) SelfID() string {
	if c == nil {
		return ""
	}
	return c.selfID
}

func votePayload(phase Phase, view int, nodeID string) string {
	return fmt.Sprintf("vote|phase=%s|view=%d|node=%s", phase, view, nodeID)
}

func (c *ReplicaCrypto) SignVote(phase Phase, view int, nodeID string) (string, error) {
	if c == nil || c.oracle == nil || c.selfID == "" {
		return "", errors.New("uninitialized replica crypto")
	}
	key, ok := c.oracle.memberKey(c.selfID)
	if !ok {
		return "", fmt.Errorf("unknown signing replica %q", c.selfID)
	}
	return computeMAC(key, votePayload(phase, view, nodeID)), nil
}

func (c *ReplicaCrypto) VerifyVote(vote Vote) bool {
	if c == nil || c.oracle == nil || vote.VoterID == "" || vote.PartialSignature == "" {
		return false
	}
	key, ok := c.oracle.memberKey(vote.VoterID)
	if !ok {
		return false
	}
	expected := computeMAC(key, votePayload(vote.Phase, vote.View, vote.NodeID))
	return hmac.Equal([]byte(expected), []byte(vote.PartialSignature))
}

// Combine returns one fixed-size aggregate authenticator. The QC does not carry
// all partial signatures, which models HotStuff's linear authenticator complexity.
func (c *ReplicaCrypto) Combine(phase Phase, view int, nodeID string, votes []Vote, quorum int) (string, error) {
	if c == nil || c.oracle == nil {
		return "", errors.New("uninitialized replica crypto")
	}
	if quorum <= 0 {
		return "", errors.New("quorum must be positive")
	}
	seen := make(map[string]Vote, len(votes))
	for _, vote := range votes {
		if vote.Phase != phase || vote.View != view || vote.NodeID != nodeID {
			return "", fmt.Errorf("vote mismatch: got (%s,%d,%s), want (%s,%d,%s)", vote.Phase, vote.View, vote.NodeID, phase, view, nodeID)
		}
		if !c.VerifyVote(vote) {
			return "", fmt.Errorf("invalid partial signature from %q", vote.VoterID)
		}
		if _, exists := seen[vote.VoterID]; exists {
			continue
		}
		seen[vote.VoterID] = vote
	}
	if len(seen) < quorum {
		return "", fmt.Errorf("not enough unique valid voters: got %d need %d", len(seen), quorum)
	}

	parts := make([]string, 0, len(seen))
	for signer, vote := range seen {
		parts = append(parts, signer+":"+vote.PartialSignature)
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(votePayload(phase, view, nodeID) + "|shares=" + strings.Join(parts, ",")))
	aggregate := hex.EncodeToString(sum[:])

	c.oracle.mu.Lock()
	c.oracle.aggregates[aggregate] = aggregateRecord{Phase: phase, View: view, NodeID: nodeID, Quorum: len(seen)}
	c.oracle.mu.Unlock()
	return aggregate, nil
}

func (c *ReplicaCrypto) VerifyQC(qc *QC, quorum int) bool {
	if qc == nil {
		return false
	}
	if qc.Genesis {
		return qc.Phase == PhaseGenesis && qc.View == 0 && qc.NodeID == GenesisID && qc.AggregateSignature == ""
	}
	if c == nil || c.oracle == nil || quorum <= 0 || qc.AggregateSignature == "" {
		return false
	}
	c.oracle.mu.RLock()
	record, ok := c.oracle.aggregates[qc.AggregateSignature]
	c.oracle.mu.RUnlock()
	return ok &&
		record.Phase == qc.Phase &&
		record.View == qc.View &&
		record.NodeID == qc.NodeID &&
		record.Quorum >= quorum
}

func (c *ReplicaCrypto) SignMessage(msg Message) (string, error) {
	if c == nil || c.oracle == nil || c.selfID == "" {
		return "", errors.New("uninitialized replica crypto")
	}
	if msg.From != c.selfID {
		return "", fmt.Errorf("replica %s cannot authenticate a message claiming sender %s", c.selfID, msg.From)
	}
	key, ok := c.oracle.memberKey(c.selfID)
	if !ok {
		return "", fmt.Errorf("unknown sender %q", c.selfID)
	}
	return computeMAC(key, msg.Canonical()), nil
}

func (c *ReplicaCrypto) VerifyMessage(msg Message) bool {
	if c == nil || c.oracle == nil || msg.From == "" || msg.AuthTag == "" {
		return false
	}
	key, ok := c.oracle.memberKey(msg.From)
	if !ok {
		return false
	}
	expected := computeMAC(key, msg.Canonical())
	return hmac.Equal([]byte(expected), []byte(msg.AuthTag))
}

func (o *SimulatedThresholdOracle) memberKey(id string) ([]byte, bool) {
	if o == nil {
		return nil, false
	}
	o.mu.RLock()
	key, ok := o.memberKeys[id]
	o.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return append([]byte(nil), key...), true
}

func computeMAC(key []byte, payload string) string {
	mac := hmac.New(sha256.New, key)
	_, _ = mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}
