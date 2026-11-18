package vault

import "testing"

// Rotation: a DEK wrapped by a retired vault master key must stay readable
// after the primary key changes.
func TestMasterKeyRotationReadsRetiredWraps(t *testing.T) {
	oldKey := make([]byte, 32)
	newKey := make([]byte, 32)
	for i := range oldKey {
		oldKey[i] = 0x11
		newKey[i] = 0x22
	}

	oldMK, err := NewMasterKey(oldKey)
	if err != nil {
		t.Fatal(err)
	}
	plainDEK, wrapped, err := oldMK.GenerateDEK()
	if err != nil {
		t.Fatal(err)
	}

	rotating, err := NewMasterKeyWithPrevious(newKey, [][]byte{oldKey})
	if err != nil {
		t.Fatal(err)
	}
	got, err := rotating.UnwrapDEK(wrapped)
	if err != nil {
		t.Fatalf("unwrap DEK wrapped by retired key: %v", err)
	}
	if len(got) != len(plainDEK) {
		t.Fatalf("DEK length = %d, want %d", len(got), len(plainDEK))
	}
	for i := range got {
		if got[i] != plainDEK[i] {
			t.Fatalf("DEK mismatch at byte %d", i)
		}
	}

	// New wraps use the primary key only.
	_, freshWrapped, err := rotating.GenerateDEK()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := oldMK.UnwrapDEK(freshWrapped); err == nil {
		t.Fatal("retired master key unwrapped a value written by the new key")
	}

	// Derivation always follows the primary key, so capability signing keys
	// rotate with it.
	if string(rotating.DeriveCSK("t1")) == string(oldMK.DeriveCSK("t1")) {
		t.Fatal("CSK derivation still uses the retired key")
	}
}
