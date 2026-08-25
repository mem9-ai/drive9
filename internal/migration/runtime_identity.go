package migration

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

var (
	ErrCredentialChanged  = errors.New("credential file changed during read")
	ErrSourceMountChanged = errors.New("mounted source identity changed")
	ebsVolumeIDPattern    = regexp.MustCompile(`(?i)(?:^|[^a-z0-9])vol-?([0-9a-f]+)(?:$|[^a-z0-9])`)
)

type sourceMountIdentity struct {
	Device                 uint64
	Inode                  uint64
	VolumeSerial           string
	VolumeIdentityVerified bool
}

type sourceMountProbe func(root, volumeID string) (sourceMountIdentity, error)

func sourceMountProbeFor(startup *Startup) sourceMountProbe {
	if startup != nil && startup.mountProbe != nil {
		return startup.mountProbe
	}
	return observeMountedSource
}

func observeJobSource(startup *Startup, probe sourceMountProbe) (sourceMountIdentity, error) {
	if startup == nil {
		return sourceMountIdentity{}, errors.New("missing startup")
	}
	ebsRoot := startup.Job.EBSRoot
	if ebsRoot == "" {
		ebsRoot = startup.Job.Source.Root
	}
	ebsIdentity, err := probe(ebsRoot, startup.Job.VolumeID)
	if err != nil {
		return sourceMountIdentity{}, err
	}
	if ebsRoot == startup.Job.Source.Root {
		return ebsIdentity, nil
	}
	sourceIdentity, err := observeSubpathRoot(ebsRoot, startup.Job.Source.Root)
	if err != nil {
		return sourceMountIdentity{}, err
	}
	if sourceIdentity.Device != ebsIdentity.Device {
		return sourceMountIdentity{}, fmt.Errorf("%w: source subpath is on another device", ErrSourceMountChanged)
	}
	sourceIdentity.VolumeSerial = ebsIdentity.VolumeSerial
	sourceIdentity.VolumeIdentityVerified = ebsIdentity.VolumeIdentityVerified
	return sourceIdentity, nil
}

func observeSubpathRoot(ebsRoot, sourceRoot string) (sourceMountIdentity, error) {
	relative, err := filepath.Rel(ebsRoot, sourceRoot)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return sourceMountIdentity{}, fmt.Errorf("%w: source subpath escapes EBS root", ErrSourceMountChanged)
	}
	root, err := os.OpenRoot(ebsRoot)
	if err != nil {
		return sourceMountIdentity{}, fmt.Errorf("open EBS root: %w", err)
	}
	defer func() { _ = root.Close() }()
	info, err := root.Lstat(relative)
	if err != nil {
		return sourceMountIdentity{}, fmt.Errorf("lstat source subpath root: %w", err)
	}
	if !info.IsDir() {
		return sourceMountIdentity{}, errors.New("source subpath root is not a directory")
	}
	identity, err := defaultFileIdentity(sourceRoot, info)
	if err != nil {
		return sourceMountIdentity{}, fmt.Errorf("source subpath root identity: %w", err)
	}
	return sourceMountIdentity{Device: identity.version.Device, Inode: identity.version.Inode}, nil
}

func observeSourceRoot(root string) (sourceMountIdentity, error) {
	info, err := os.Lstat(root)
	if err != nil {
		return sourceMountIdentity{}, fmt.Errorf("lstat source root: %w", err)
	}
	if !info.IsDir() {
		return sourceMountIdentity{}, errors.New("source root is not a directory")
	}
	identity, err := defaultFileIdentity(root, info)
	if err != nil {
		return sourceMountIdentity{}, fmt.Errorf("source root identity: %w", err)
	}
	return sourceMountIdentity{Device: identity.version.Device, Inode: identity.version.Inode}, nil
}

func observeMountedSource(root, volumeID string) (sourceMountIdentity, error) {
	identity, err := observeSourceRoot(root)
	if err != nil {
		return sourceMountIdentity{}, err
	}
	serial, available, err := platformVolumeSerial(root)
	if err != nil {
		return sourceMountIdentity{}, err
	}
	if !available {
		return identity, nil
	}
	return verifyMountedVolumeSerial(identity, volumeID, serial)
}

func verifyMountedVolumeSerial(identity sourceMountIdentity, volumeID, serial string) (sourceMountIdentity, error) {
	identity.VolumeSerial = extractEBSVolumeID(serial)
	if identity.VolumeSerial == "" || identity.VolumeSerial != canonicalVolumeID(volumeID) {
		return sourceMountIdentity{}, fmt.Errorf("%w: mounted volume serial does not match volume_id", ErrSourceMountChanged)
	}
	identity.VolumeIdentityVerified = true
	return identity, nil
}

func extractEBSVolumeID(value string) string {
	match := ebsVolumeIDPattern.FindStringSubmatch(strings.TrimSpace(value))
	if len(match) != 2 {
		return ""
	}
	return "vol-" + strings.ToLower(match[1])
}

func canonicalVolumeID(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if !volumeIDPattern.MatchString(value) {
		return ""
	}
	return value
}

func (accepted sourceMountIdentity) matches(observed sourceMountIdentity) bool {
	if accepted.Device != observed.Device || accepted.Inode != observed.Inode {
		return false
	}
	if accepted.VolumeIdentityVerified {
		return observed.VolumeIdentityVerified && accepted.VolumeSerial == observed.VolumeSerial
	}
	return true
}

func (identity sourceMountIdentity) present() bool {
	return identity.Device != 0 || identity.Inode != 0
}

type credentialFingerprint struct {
	Device  uint64
	Inode   uint64
	Size    int64
	MtimeNS int64
	CtimeNS int64
	Mode    uint32
}

func credentialFileFingerprint(path string) (credentialFingerprint, error) {
	info, err := os.Stat(path)
	if err != nil {
		return credentialFingerprint{}, err
	}
	identity, err := defaultFileIdentity(path, info)
	if err != nil {
		return credentialFingerprint{}, err
	}
	return credentialFingerprint{
		Device: identity.version.Device, Inode: identity.version.Inode,
		Size: identity.version.Size, MtimeNS: identity.version.MtimeNS,
		CtimeNS: identity.version.CtimeNS, Mode: identity.version.Mode,
	}, nil
}

func (s CredentialSource) fingerprint() (credentialFingerprint, error) {
	return credentialFileFingerprint(s.path)
}

func (s CredentialSource) readStable() (string, credentialFingerprint, error) {
	before, err := s.fingerprint()
	if err != nil {
		return "", credentialFingerprint{}, fmt.Errorf("stat credential %q: %w", filepath.Base(s.path), err)
	}
	key, err := s.Read()
	if err != nil {
		return "", credentialFingerprint{}, err
	}
	after, err := s.fingerprint()
	if err != nil {
		return "", credentialFingerprint{}, fmt.Errorf("restat credential %q: %w", filepath.Base(s.path), err)
	}
	if before != after {
		return "", credentialFingerprint{}, ErrCredentialChanged
	}
	return key, after, nil
}
