package hotstuff

// SafeNode is the Basic HotStuff voting rule. It returns true if either:
//   - the proposal extends the replica's currently locked node, or
//   - the proposal is justified by a QC from a higher view than the lock.
//
// Callers must first validate the proposal and rebuild its ancestry from the
// local tree; transmitted ancestor lists are never trusted.
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
