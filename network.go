package main

import (
	"math/rand"
	"sync"
	"time"
)

type Network struct {
	mu             sync.RWMutex
	NodeChannels   map[string]chan Message
	ClientChannels map[string]chan Command
	Crashed        map[string]bool
	NodeStore      map[string]*TreeNode
	NodeIDs        []string
	DropRate       float64
	DelayMax       int

	visualEvents []ProtocolEvent
	nextEventID  uint64
}

func NewNetwork(nodeIDs []string, dropRate float64, delayMax int) *Network {
	return &Network{
		NodeChannels:   make(map[string]chan Message),
		ClientChannels: make(map[string]chan Command),
		Crashed:        make(map[string]bool),
		NodeStore:      make(map[string]*TreeNode),
		NodeIDs:        append([]string(nil), nodeIDs...),
		DropRate:       dropRate,
		DelayMax:       delayMax,
	}
}

func (net *Network) RegisterNode(nodeID string) (chan Message, chan Command) {
	net.mu.Lock()
	defer net.mu.Unlock()

	messageChannel := make(chan Message, 256)
	clientChannel := make(chan Command, 64)
	net.NodeChannels[nodeID] = messageChannel
	net.ClientChannels[nodeID] = clientChannel
	return messageChannel, clientChannel
}

func (net *Network) LeaderForView(viewNumber int) string {
	if viewNumber < 1 || len(net.NodeIDs) == 0 {
		return ""
	}
	return net.NodeIDs[(viewNumber-1)%len(net.NodeIDs)]
}

func (net *Network) Send(senderID, receiverID string, msg Message) {
	net.mu.RLock()
	senderCrashed := net.Crashed[senderID]
	receiverCrashed := net.Crashed[receiverID]
	_, senderKnown := net.NodeChannels[senderID]
	channel, receiverKnown := net.NodeChannels[receiverID]
	dropRate := net.DropRate
	delayMax := net.DelayMax
	net.mu.RUnlock()

	event := protocolEventFromMessage(senderID, receiverID, msg)
	if !senderKnown || !receiverKnown {
		event.DeliveryState = "rejected"
		event.Detail = "unknown transport endpoint"
		net.RecordEvent(event)
		return
	}
	if senderCrashed || receiverCrashed {
		event.DeliveryState = "dropped"
		event.Detail = "sender or receiver is crashed"
		net.RecordEvent(event)
		return
	}
	if dropRate > 0 && rand.Float64() < dropRate {
		event.DeliveryState = "dropped"
		event.Detail = "network drop"
		net.RecordEvent(event)
		return
	}

	msg = cloneMessage(msg)
	// The transport, not the caller-supplied message, stamps the authenticated
	// sender identity.
	msg.SenderID = senderID
	event.DeliveryState = "sent"
	net.RecordEvent(event)

	delay := 0
	if delayMax > 0 {
		delay = rand.Intn(delayMax + 1)
	}

	go func() {
		time.Sleep(time.Duration(delay) * time.Millisecond)
		net.mu.RLock()
		stillAvailable := !net.Crashed[receiverID]
		net.mu.RUnlock()
		if !stillAvailable {
			return
		}
		channel <- msg
	}()
}

func protocolEventFromMessage(senderID, receiverID string, msg Message) ProtocolEvent {
	nodeHash := ""
	if msg.Node != nil {
		nodeHash = msg.Node.Hash
	} else if msg.Justify != nil {
		nodeHash = msg.Justify.NodeHash
	}
	event := ProtocolEvent{
		Kind:       "protocol",
		From:       senderID,
		To:         receiverID,
		Type:       msg.Type,
		ViewNumber: msg.ViewNumber,
		NodeHash:   nodeHash,
		IsVote:     msg.IsVote,
	}
	if msg.Justify != nil {
		event.JustifyType = msg.Justify.Type
		event.JustifyView = msg.Justify.ViewNumber
	}
	return event
}

func (net *Network) Broadcast(senderID string, msg Message) {
	net.mu.RLock()
	nodeIDs := append([]string(nil), net.NodeIDs...)
	net.mu.RUnlock()
	for _, nodeID := range nodeIDs {
		net.Send(senderID, nodeID, msg)
	}
}

func (net *Network) BroadcastClientCommand(cmd Command) {
	net.mu.RLock()
	channels := make(map[string]chan Command, len(net.ClientChannels))
	for id, channel := range net.ClientChannels {
		if !net.Crashed[id] {
			channels[id] = channel
		}
	}
	net.mu.RUnlock()

	for nodeID, channel := range channels {
		net.RecordEvent(ProtocolEvent{
			Kind:          "client-command",
			From:          "Client",
			To:            nodeID,
			CommandID:     cmd.ID,
			CommandType:   string(cmd.Type),
			DeliveryState: "sent",
		})
		channel <- cmd
	}
}

func (net *Network) CrashNode(nodeID string) {
	net.mu.Lock()
	net.Crashed[nodeID] = true
	net.recordEventLocked(ProtocolEvent{
		Kind:          "system",
		From:          "System",
		To:            nodeID,
		DeliveryState: "system",
		Detail:        "node crashed",
	})
	net.mu.Unlock()
}

func (net *Network) StoreNode(node *TreeNode) {
	if node == nil || node.Hash == "" {
		return
	}
	net.mu.Lock()
	if _, exists := net.NodeStore[node.Hash]; !exists {
		net.NodeStore[node.Hash] = cloneTreeNode(node)
	}
	net.mu.Unlock()
}

func (net *Network) FetchNode(nodeHash string) (*TreeNode, bool) {
	net.mu.RLock()
	node, exists := net.NodeStore[nodeHash]
	net.mu.RUnlock()
	if !exists {
		return nil, false
	}
	return cloneTreeNode(node), true
}

func (net *Network) RecordEvent(event ProtocolEvent) {
	net.mu.Lock()
	net.recordEventLocked(event)
	net.mu.Unlock()
}

func (net *Network) recordEventLocked(event ProtocolEvent) {
	net.nextEventID++
	event.Sequence = net.nextEventID
	event.Timestamp = time.Now().UTC().Format(time.RFC3339Nano)
	net.visualEvents = append(net.visualEvents, event)
	if len(net.visualEvents) > maxVisualEvents {
		start := len(net.visualEvents) - maxVisualEvents
		trimmed := make([]ProtocolEvent, maxVisualEvents)
		copy(trimmed, net.visualEvents[start:])
		net.visualEvents = trimmed
	}
}

func (net *Network) SnapshotEvents(limit int) []ProtocolEvent {
	net.mu.RLock()
	defer net.mu.RUnlock()
	start := 0
	if limit > 0 && len(net.visualEvents) > limit {
		start = len(net.visualEvents) - limit
	}
	result := make([]ProtocolEvent, len(net.visualEvents)-start)
	copy(result, net.visualEvents[start:])
	return result
}

func (net *Network) SnapshotNodeStore() map[string]*TreeNode {
	net.mu.RLock()
	defer net.mu.RUnlock()
	result := make(map[string]*TreeNode, len(net.NodeStore))
	for hash, node := range net.NodeStore {
		result[hash] = cloneTreeNode(node)
	}
	return result
}

func (net *Network) SnapshotCrashed() map[string]bool {
	net.mu.RLock()
	defer net.mu.RUnlock()
	result := make(map[string]bool, len(net.Crashed))
	for id, crashed := range net.Crashed {
		result[id] = crashed
	}
	return result
}

func cloneMessage(msg Message) Message {
	clone := msg
	clone.Node = cloneTreeNode(msg.Node)
	clone.Justify = cloneQC(msg.Justify)
	if msg.PartialSig != nil {
		share := cloneSignatureShare(*msg.PartialSig)
		clone.PartialSig = &share
	}
	return clone
}

func cloneTreeNode(node *TreeNode) *TreeNode {
	if node == nil {
		return nil
	}
	clone := *node
	return &clone
}

func cloneQC(qc *QuorumCertificate) *QuorumCertificate {
	if qc == nil {
		return nil
	}
	clone := *qc
	return &clone
}
