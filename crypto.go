package main

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
)

type Signer struct {
	id         string
	privateKey ed25519.PrivateKey
}

type certificateRecord struct {
	Type       MsgType
	ViewNumber int
	NodeHash   string
	Shares     map[string]SignatureShare
}

type ThresholdVerifier struct {
	mu           sync.RWMutex
	publicKeys   map[string]ed25519.PublicKey
	quorum       int
	certificates map[string]certificateRecord
}

func NewCryptoSuite(nodeIDs []string, quorum int) (map[string]*Signer, *ThresholdVerifier, error) {
	signers := make(map[string]*Signer, len(nodeIDs))
	publicKeys := make(map[string]ed25519.PublicKey, len(nodeIDs))

	for _, id := range nodeIDs {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, nil, fmt.Errorf("generate key for %s: %w", id, err)
		}
		signers[id] = &Signer{id: id, privateKey: privateKey}
		publicKeys[id] = publicKey
	}

	return signers, &ThresholdVerifier{
		publicKeys:   publicKeys,
		quorum:       quorum,
		certificates: make(map[string]certificateRecord),
	}, nil
}

func votePayload(messageType MsgType, viewNumber int, nodeHash string) []byte {
	payload, err := json.Marshal(struct {
		Type       MsgType `json:"type"`
		ViewNumber int     `json:"view_number"`
		NodeHash   string  `json:"node_hash"`
	}{
		Type:       messageType,
		ViewNumber: viewNumber,
		NodeHash:   nodeHash,
	})
	if err != nil {
		panic(err)
	}
	return payload
}

func (s *Signer) Sign(messageType MsgType, viewNumber int, nodeHash string) SignatureShare {
	return SignatureShare{
		SignerID:  s.id,
		Signature: ed25519.Sign(s.privateKey, votePayload(messageType, viewNumber, nodeHash)),
	}
}

func (v *ThresholdVerifier) IsMember(nodeID string) bool {
	v.mu.RLock()
	defer v.mu.RUnlock()
	_, exists := v.publicKeys[nodeID]
	return exists
}

func (v *ThresholdVerifier) VerifyShare(
	share SignatureShare,
	messageType MsgType,
	viewNumber int,
	nodeHash string,
) bool {
	v.mu.RLock()
	publicKey, exists := v.publicKeys[share.SignerID]
	v.mu.RUnlock()
	if !exists {
		return false
	}
	return ed25519.Verify(publicKey, votePayload(messageType, viewNumber, nodeHash), share.Signature)
}

func (v *ThresholdVerifier) Combine(
	messageType MsgType,
	viewNumber int,
	nodeHash string,
	shares map[string]SignatureShare,
) (string, error) {
	if len(shares) < v.quorum {
		return "", fmt.Errorf("need %d unique shares, got %d", v.quorum, len(shares))
	}

	signerIDs := make([]string, 0, len(shares))
	verified := make(map[string]SignatureShare, len(shares))
	for signerID, share := range shares {
		if signerID != share.SignerID {
			return "", errors.New("share map key does not match signer identity")
		}
		if !v.VerifyShare(share, messageType, viewNumber, nodeHash) {
			return "", fmt.Errorf("invalid share from %s", signerID)
		}
		signerIDs = append(signerIDs, signerID)
		verified[signerID] = cloneSignatureShare(share)
	}
	sort.Strings(signerIDs)

	hashInput := append([]byte{}, votePayload(messageType, viewNumber, nodeHash)...)
	for _, signerID := range signerIDs {
		hashInput = append(hashInput, []byte(signerID)...)
		hashInput = append(hashInput, verified[signerID].Signature...)
	}
	digest := sha256.Sum256(hashInput)
	token := hex.EncodeToString(digest[:])

	v.mu.Lock()
	v.certificates[token] = certificateRecord{
		Type:       messageType,
		ViewNumber: viewNumber,
		NodeHash:   nodeHash,
		Shares:     verified,
	}
	v.mu.Unlock()

	return token, nil
}

func (v *ThresholdVerifier) VerifyQC(qc *QuorumCertificate) bool {
	if qc == nil || qc.ThresholdSignature == "" {
		return false
	}

	v.mu.RLock()
	record, exists := v.certificates[qc.ThresholdSignature]
	v.mu.RUnlock()
	if !exists {
		return false
	}
	if record.Type != qc.Type || record.ViewNumber != qc.ViewNumber || record.NodeHash != qc.NodeHash {
		return false
	}
	if len(record.Shares) < v.quorum {
		return false
	}
	for signerID, share := range record.Shares {
		if signerID != share.SignerID || !v.VerifyShare(share, qc.Type, qc.ViewNumber, qc.NodeHash) {
			return false
		}
	}
	return true
}

func cloneSignatureShare(share SignatureShare) SignatureShare {
	return SignatureShare{
		SignerID:  share.SignerID,
		Signature: append([]byte(nil), share.Signature...),
	}
}

func (v *ThresholdVerifier) SnapshotCertificates() []QuorumCertificate {
	v.mu.RLock()
	certificates := make([]QuorumCertificate, 0, len(v.certificates))
	for token, record := range v.certificates {
		certificates = append(certificates, QuorumCertificate{
			Type:               record.Type,
			ViewNumber:         record.ViewNumber,
			NodeHash:           record.NodeHash,
			ThresholdSignature: token,
		})
	}
	v.mu.RUnlock()

	sort.Slice(certificates, func(i, j int) bool {
		if certificates[i].ViewNumber == certificates[j].ViewNumber {
			if certificates[i].Type == certificates[j].Type {
				return certificates[i].NodeHash < certificates[j].NodeHash
			}
			return certificates[i].Type < certificates[j].Type
		}
		return certificates[i].ViewNumber < certificates[j].ViewNumber
	})
	return certificates
}
