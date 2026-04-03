package cli

import (
	"errors"
	"reflect"
	"testing"
)

func fakeLookPath(binMap map[string]bool) func(string) (string, error) {
	return func(name string) (string, error) {
		if binMap[name] {
			return "/usr/bin/" + name, nil
		}
		return "", errors.New("not found")
	}
}

func TestUmountArgvDarwin(t *testing.T) {
	got, err := umountArgv("darwin", fakeLookPath(nil), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"umount", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvPrefersFusermount3(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"fusermount3": true,
		"fusermount":  true,
		"umount":      true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"fusermount3", "-u", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvFallsBackToFusermount(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"fusermount": true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"fusermount", "-u", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvFallsBackToUmount(t *testing.T) {
	got, err := umountArgv("linux", fakeLookPath(map[string]bool{
		"umount": true,
	}), "/mnt/drive9")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []string{"umount", "/mnt/drive9"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("argv = %v, want %v", got, want)
	}
}

func TestUmountArgvNoBinary(t *testing.T) {
	_, err := umountArgv("linux", fakeLookPath(nil), "/mnt/drive9")
	if err == nil {
		t.Fatal("expected error when no unmount binaries are available")
	}
}
