package hotstuff

import "testing"

func TestBranchExtensionAndConflictDetection(t *testing.T) {
	tree := NewTree()
	root := tree.Genesis()
	a := NewNode(root, testCommand("a"), "B1", 1)
	b := NewNode(a, testCommand("b"), "B2", 2)
	c := NewNode(root, testCommand("c"), "B3", 2)
	tree.Add(a)
	tree.Add(b)
	tree.Add(c)
	if !tree.Extends(b.ID, a.ID) {
		t.Fatal("expected b to extend a")
	}
	if tree.Conflicts(a.ID, b.ID) {
		t.Fatal("ancestor and child should not conflict")
	}
	if !tree.Conflicts(b.ID, c.ID) {
		t.Fatal("sibling branches should conflict")
	}
}
