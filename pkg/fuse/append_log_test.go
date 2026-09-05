package fuse

import (
	"testing"

	"github.com/mem9-ai/drive9/pkg/client"
)

func TestAppendLogHandleMutation(t *testing.T) {
	tests := []struct {
		name   string
		isNew  bool
		writes [][2]int64
		want   bool
	}{
		{name: "existing pure extension", writes: [][2]int64{{100, 8}, {108, 4}}, want: true},
		{name: "existing overwrite", writes: [][2]int64{{99, 8}, {108, 4}}, want: false},
		{name: "existing gap", writes: [][2]int64{{101, 8}}, want: false},
		{name: "existing backwrite", writes: [][2]int64{{100, 8}, {101, 4}}, want: false},
		{name: "new file random assembly", isNew: true, writes: [][2]int64{{20, 4}, {0, 8}}, want: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fh := &FileHandle{IsNew: test.isNew, OrigSize: 100}
			size := fh.OrigSize
			for _, write := range test.writes {
				fh.appendLogRecordUserWrite(size, write[0], write[1])
				if end := write[0] + write[1]; end > size {
					size = end
				}
			}
			if got := fh.appendLogCanUseTail(); got != test.want {
				t.Fatalf("appendLogCanUseTail() = %t, want %t", got, test.want)
			}
		})
	}
}

func TestAppendLogHandleTruncate(t *testing.T) {
	existing := &FileHandle{OrigSize: 100}
	existing.appendLogRecordUserWrite(100, 100, 8)
	existing.appendLogRecordTruncate()
	existing.appendLogRecordUserWrite(0, 0, 4)
	if existing.appendLogCanUseTail() {
		t.Fatal("existing truncate must permanently invalidate its dirty generation")
	}

	created := &FileHandle{IsNew: true}
	created.appendLogRecordTruncate()
	created.appendLogRecordUserWrite(0, 12, 4)
	if !created.appendLogCanUseTail() {
		t.Fatal("new file truncate must remain eligible for append-log creation")
	}
}

func TestAppendLogLayout(t *testing.T) {
	fh := &FileHandle{}
	fh.appendLogObserveLayout(client.ContentLayoutSingle, 4, 100)
	if got := fh.appendLogLayoutAt(4, 100); got != client.ContentLayoutSingle {
		t.Fatalf("layout = %q, want single", got)
	}
	if got := fh.appendLogLayoutAt(5, 100); got != "" {
		t.Fatalf("stale-revision layout = %q, want unknown", got)
	}

	fh.appendLogMarkAppendSuccess(6, 108)
	if got := fh.appendLogLayoutAt(6, 108); got != client.ContentLayoutAppendLog {
		t.Fatalf("append success layout = %q, want append_log", got)
	}
	if !fh.appendLogCanUseTail() {
		t.Fatal("append success must establish a clean append-safe baseline")
	}

	fh.appendLogMarkUnsupported()
	if fh.appendLogCanUseTail() {
		t.Fatal("append_log_unsupported must suppress later append attempts on the handle")
	}
	if got := fh.appendLogLayoutAt(6, 108); got != client.ContentLayoutAppendLog {
		t.Fatalf("unsupported must not overwrite known layout: got %q", got)
	}
}

func TestAppendLogBaseline(t *testing.T) {
	fh := &FileHandle{OrigSize: 100}
	fh.appendLogObserveLayout(client.ContentLayoutAppendLog, 4, 100)
	fh.appendLogRecordUserWrite(100, 90, 4)
	fh.appendLogMarkUnsupported()
	fh.appendLogAdoptCommittedBaseline(5, 104)

	if !fh.appendLog.appendSafe {
		t.Fatal("committed baseline must reset append safety")
	}
	if !fh.appendLog.unsupported {
		t.Fatal("committed baseline must preserve append unsupported suppression")
	}
	if got := fh.appendLogLayoutAt(5, 104); got != client.ContentLayoutAppendLog {
		t.Fatalf("baseline layout = %q, want append_log", got)
	}
}
