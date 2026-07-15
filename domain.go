package main

const GenesisHash = "GENESIS"

type CommandType string

const (
	Transfer     CommandType = "TRANSFER"
	BlockAccount CommandType = "BLOCK_ACCOUNT"
	ApproveLoan  CommandType = "APPROVE_LOAN"
)

type Command struct {
	ID       string      `json:"id"`
	Type     CommandType `json:"type"`
	From     string      `json:"from"`
	To       string      `json:"to"`
	Amount   int         `json:"amount"`
	Metadata string      `json:"metadata"`
}

type MsgType string

const (
	NewView   MsgType = "NEW-VIEW"
	Prepare   MsgType = "PREPARE"
	PreCommit MsgType = "PRE-COMMIT"
	Commit    MsgType = "COMMIT"
	Decide    MsgType = "DECIDE"
)

type TreeNode struct {
	ParentHash   string  `json:"parent_hash"`
	Cmd          Command `json:"command"`
	Hash         string  `json:"hash"`
	ProposedView int     `json:"proposed_view"`
}

// QuorumCertificate models the paper's QC over
// <type, viewNumber, node>. ThresholdSignature is one opaque combined
// authenticator produced by ThresholdVerifier.Combine.
type QuorumCertificate struct {
	Type               MsgType `json:"type"`
	ViewNumber         int     `json:"view_number"`
	NodeHash           string  `json:"node_hash"`
	ThresholdSignature string  `json:"threshold_signature"`
}

type SignatureShare struct {
	SignerID  string `json:"signer_id"`
	Signature []byte `json:"signature"`
}

type Message struct {
	Type       MsgType            `json:"type"`
	ViewNumber int                `json:"view_number"`
	Node       *TreeNode          `json:"node,omitempty"`
	Justify    *QuorumCertificate `json:"justify,omitempty"`
	SenderID   string             `json:"sender_id"`
	PartialSig *SignatureShare    `json:"partial_signature,omitempty"`
	IsVote     bool               `json:"is_vote"`
}
