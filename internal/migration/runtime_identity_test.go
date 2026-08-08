package migration

import "testing"

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
	if serial := extractEBSVolumeID("nvme-Amazon_Elastic_Block_Store_vol0123abcd"); serial == canonicalVolumeID("vol-0123") {
		t.Fatal("short configured volume ID matched a longer serial")
	}
}
