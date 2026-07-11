package hotstuff

import "testing"

func TestBranchExtensionAndConflictDetection(t *testing.T) {
	tree := NewTree()
	root := tree.Genesis()
	a := NewNode(root, testCommand("a"), "B1", 1)
	if _, err := tree.AddValidated(a); err != nil {
		t.Fatal(err)
	}
	b := NewNode(a, testCommand("b"), "B2", 2)
	c := NewNode(root, testCommand("c"), "B3", 2)
	if _, err := tree.AddValidated(b); err != nil {
		t.Fatal(err)
	}
	if _, err := tree.AddValidated(c); err != nil {
		t.Fatal(err)
	}
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

func TestTreeRejectsForgedAncestryAndTamperedNode(t *testing.T) {
	tree := NewTree()
	root := tree.Genesis()
	locked := NewNode(root, testCommand("locked"), "B1", 1)
	if _, err := tree.AddValidated(locked); err != nil {
		t.Fatal(err)
	}
	forged := NewNode(root, testCommand("conflict"), "B2", 2)
	forged.Ancestors = append(forged.Ancestors, locked.ID)
	prepared, err := tree.PrepareNode(forged)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.Extends(locked.ID) {
		t.Fatal("transmitted ancestor list must be ignored and rebuilt from parent links")
	}

	tampered := forged.Clone()
	tampered.Command = testCommand("changed-after-hash")
	if _, err := tree.PrepareNode(tampered); err == nil {
		t.Fatal("tampered node payload must fail hash validation")
	}
}
