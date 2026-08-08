package migration

import (
	"errors"
	"testing"
)

func TestExtractEBSVolumeIDRequiresExactToken(t *testing.T) {
	for _, tc := range []struct {
		serial string
		want   string
	}{
		{serial: "vol-0123abcd", want: "vol-0123abcd"},
		{serial: "vol0123abcd", want: "vol-0123abcd"},
		{serial: "nvme-Amazon_Elastic_Block_Store_vol0123abcd", want: "vol-0123abcd"},
		{serial: "nvme-Amazon_Elastic_Block_Store_vol0123abcd-part1", want: "vol-0123abcd"},
		{serial: "prefixvol0123abcd", want: ""},
		{serial: "vol0123abcdsuffix", want: ""},
	} {
		if got := extractEBSVolumeID(tc.serial); got != tc.want {
			t.Fatalf("extractEBSVolumeID(%q)=%q, want %q", tc.serial, got, tc.want)
		}
	}
}

func TestVerifyMountedVolumeSerialReturnsTypedMismatch(t *testing.T) {
	identity := sourceMountIdentity{Device: 1, Inode: 2}
	verified, err := verifyMountedVolumeSerial(identity, "vol-0123abcd", "nvme-Amazon_Elastic_Block_Store_vol0123abcd")
	if err != nil || !verified.VolumeIdentityVerified || verified.Device != identity.Device || verified.Inode != identity.Inode {
		t.Fatalf("verified identity=%+v err=%v", verified, err)
	}
	if _, err := verifyMountedVolumeSerial(identity, "vol-0123", "nvme-Amazon_Elastic_Block_Store_vol0123abcd"); !errors.Is(err, ErrSourceMountChanged) {
		t.Fatalf("volume mismatch error=%v", err)
	}
}
