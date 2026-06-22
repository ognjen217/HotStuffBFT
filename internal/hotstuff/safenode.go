package hotstuff

// SafeNode is the Basic HotStuff voting rule. It returns true if either:
//   - the proposal extends the replica's currently locked node, or
//   - the proposal is justified by a QC from a higher view than the lock.
//
// A nil/genesis lockedQC is treated as no lock. This simulator models QC
// semantics but does not implement production threshold cryptography.
func SafeNode(node *Node, justifyQC *QC, lockedQC *QC) bool {
	if node == nil {
		return false
	}
	if lockedQC == nil || lockedQC.Genesis || lockedQC.NodeID == "" || lockedQC.NodeID == GenesisID {
		return true
	}
	if node.Extends(lockedQC.NodeID) {
		return true
	}
	return justifyQC != nil && justifyQC.View > lockedQC.View
}
