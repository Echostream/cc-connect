//go:build linux || darwin

package core

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

type fileIdentity struct {
	dev     uint64
	ino     uint64
	uid     uint32
	size    int64
	mode    os.FileMode
	regular bool
	dir     bool
}

func identityFromInfo(info os.FileInfo) (fileIdentity, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fileIdentity{}, stagedAttachmentError{reason: stagedReasonWriteFailed}
	}
	return fileIdentity{
		dev:     uint64(stat.Dev),
		ino:     uint64(stat.Ino),
		uid:     uint32(stat.Uid),
		size:    info.Size(),
		mode:    info.Mode().Perm(),
		regular: info.Mode().IsRegular(),
		dir:     info.IsDir(),
	}, nil
}

func identityFromRoot(root *os.Root, name string) (fileIdentity, error) {
	info, err := root.Lstat(name)
	if err != nil {
		return fileIdentity{}, err
	}
	return identityFromInfo(info)
}

func (id fileIdentity) sameFile(other fileIdentity) bool {
	return id.dev == other.dev && id.ino == other.ino
}

func currentEUID() uint32 { return uint32(os.Geteuid()) }

func openRootCreateNoFollow(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL|unix.O_NOFOLLOW, 0o644)
}

func openRootRegularNoFollow(root *os.Root, name string) (*os.File, error) {
	return root.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
}

func fsyncRootDir(root *os.Root, name string) error {
	dir, err := root.Open(name)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
