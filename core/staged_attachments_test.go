//go:build linux || darwin

package core

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
)

func stagedReason(t *testing.T, err error) string {
	t.Helper()
	if err == nil {
		return ""
	}
	var reason interface{ Reason() string }
	if !errors.As(err, &reason) {
		t.Fatalf("error %T does not expose safe Reason(): %v", err, err)
	}
	return reason.Reason()
}

func mustStage(t *testing.T, workDir string, msg *Message, maxBytes int64) *StagedAttachments {
	t.Helper()
	staged, err := StageMessageAttachments(workDir, msg, maxBytes)
	if err != nil {
		t.Fatalf("StageMessageAttachments() error = %v", err)
	}
	if staged == nil {
		t.Fatal("StageMessageAttachments() returned nil staged attachments")
	}
	t.Cleanup(func() { _ = staged.Cleanup() })
	return staged
}

func TestDefaultMaxAttachmentSizeIsFiftyMiBAndCannotBeRaisedByStaging(t *testing.T) {
	// Catches accidental drift of the production attachment default or staging
	// code that treats a caller-provided larger cap as permission to exceed it.
	if DefaultMaxAttachmentSize != 50<<20 {
		t.Fatalf("DefaultMaxAttachmentSize = %d, want %d", DefaultMaxAttachmentSize, int64(50<<20))
	}
	msg := &Message{MessageID: "default-cap", Files: []FileAttachment{{Data: []byte("12345"), FileName: "too-large-for-effective-cap.txt"}}}
	staged, err := stageMessageAttachmentsWithOps(t.TempDir(), msg, 100, stagedFileOps{maxBytesCeiling: 4})
	if got := stagedReason(t, err); got != "attachment_too_large" {
		t.Fatalf("reason = %q, want attachment_too_large (staged=%v err=%v)", got, staged, err)
	}
}

func TestStageMessageAttachmentsValidatesWritesUnderWorkspaceAndReleasesBytes(t *testing.T) {
	// Catches implementations that keep queued image/file bytes in Message, accept
	// unsupported attachments, or write outside the workspace attachment root.
	workDir := t.TempDir()
	msg := &Message{
		MessageID: "raw/message/../id",
		Images: []ImageAttachment{{
			MimeType: "image/png",
			Data:     []byte("img"),
			FileName: "../photo.png",
		}},
		Files: []FileAttachment{{
			MimeType: "text/plain",
			Data:     []byte("file"),
			FileName: "../../secret.txt",
		}},
	}
	staged := mustStage(t, workDir, msg, 4)
	if len(msg.Images) != 0 || len(msg.Files) != 0 {
		t.Fatalf("stage success retained raw bytes: images=%d files=%d", len(msg.Images), len(msg.Files))
	}
	if msg.Staged != staged {
		t.Fatalf("Message.Staged not set to returned object")
	}

	entries := staged.Entries()
	if len(entries) != 2 {
		t.Fatalf("Entries() len = %d, want 2", len(entries))
	}
	root := filepath.Join(workDir, ".cc-connect", "attachments")
	for _, entry := range entries {
		if !filepath.IsAbs(entry.Path) {
			t.Fatalf("entry path is not absolute: %q", entry.Path)
		}
		rel, err := filepath.Rel(root, entry.Path)
		if err != nil || rel == "." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || rel == ".." {
			t.Fatalf("entry path %q escaped attachment root %q (rel=%q err=%v)", entry.Path, root, rel, err)
		}
		if strings.Contains(entry.Path, "raw/message") || strings.Contains(entry.Path, "..") {
			t.Fatalf("entry path includes unsanitized message id or traversal: %q", entry.Path)
		}
		info, err := os.Stat(entry.Path)
		if err != nil {
			t.Fatalf("stat staged file: %v", err)
		}
		if got := info.Mode().Perm(); got != 0o644 {
			t.Fatalf("file mode = %o, want 0644", got)
		}
		if got := info.Size(); got != entry.Size {
			t.Fatalf("public size = %d, stat size = %d", entry.Size, got)
		}
	}
	if got := os.FileMode(0); true {
		dirInfo, err := os.Stat(filepath.Dir(entries[0].Path))
		if err != nil {
			t.Fatalf("stat message dir: %v", err)
		}
		got = dirInfo.Mode().Perm()
		if got != 0o755 {
			t.Fatalf("message dir mode = %o, want 0755", got)
		}
	}
}

func TestStageMessageAttachmentsRejectsInvalidInputsWithoutMutatingMessage(t *testing.T) {
	// Catches validators that partially clear Message bytes on failure or expose
	// sensitive filenames/paths through returned errors.
	for _, tc := range []struct {
		name       string
		msg        Message
		maxBytes   int64
		wantReason string
		canary     string
	}{
		{
			name: "file equal limit accepted",
			msg: Message{MessageID: "ok", Files: []FileAttachment{{
				MimeType: "text/plain", Data: []byte("1234"), FileName: "ok.txt",
			}}},
			maxBytes: 4,
		},
		{
			name: "file above limit rejected",
			msg: Message{MessageID: "too-large", Files: []FileAttachment{{
				MimeType: "text/plain", Data: []byte("12345"), FileName: "CANARY-large-name.txt",
			}}},
			maxBytes:   4,
			wantReason: "attachment_too_large",
			canary:     "CANARY-large-name",
		},
		{
			name: "empty file rejected",
			msg: Message{MessageID: "empty", Files: []FileAttachment{{
				MimeType: "text/plain", Data: nil, FileName: "CANARY-empty.txt",
			}}},
			maxBytes:   4,
			wantReason: "attachment_unsupported_type",
			canary:     "CANARY-empty",
		},
		{
			name: "non image MIME rejected for image",
			msg: Message{MessageID: "bad-image", Images: []ImageAttachment{{
				MimeType: "text/plain", Data: []byte("abcd"), FileName: "CANARY-bad-image.txt",
			}}},
			maxBytes:   4,
			wantReason: "attachment_unsupported_type",
			canary:     "CANARY-bad-image",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workDir := t.TempDir()
			msg := tc.msg
			imageCount := len(msg.Images)
			fileCount := len(msg.Files)
			staged, err := StageMessageAttachments(workDir, &msg, tc.maxBytes)
			if tc.wantReason == "" {
				if err != nil {
					t.Fatalf("StageMessageAttachments() error = %v", err)
				}
				_ = staged.Cleanup()
				return
			}
			if got := stagedReason(t, err); got != tc.wantReason {
				t.Fatalf("reason = %q, want %q (err=%v)", got, tc.wantReason, err)
			}
			if tc.canary != "" && strings.Contains(err.Error(), tc.canary) {
				t.Fatalf("safe error leaked canary %q: %v", tc.canary, err)
			}
			if staged != nil || msg.Staged != nil {
				t.Fatalf("failed stage returned staged state: returned=%v msg=%v", staged, msg.Staged)
			}
			if len(msg.Images) != imageCount || len(msg.Files) != fileCount {
				t.Fatalf("failed stage mutated message attachment slices")
			}
			if _, statErr := os.Stat(filepath.Join(workDir, ".cc-connect", "attachments")); statErr == nil {
				entries, _ := os.ReadDir(filepath.Join(workDir, ".cc-connect", "attachments"))
				if len(entries) != 0 {
					t.Fatalf("failed stage left attachment entries: %v", entries)
				}
			}
		})
	}
}

func TestStageMessageAttachmentsUsesUniqueNonceDirectoryForSameMessageID(t *testing.T) {
	// Catches implementations that reuse predictable message directories and can
	// mix stale files with a new staged ownership object.
	workDir := t.TempDir()
	first := mustStage(t, workDir, &Message{MessageID: "same", Files: []FileAttachment{{Data: []byte("one"), FileName: "same.txt"}}}, 10)
	second := mustStage(t, workDir, &Message{MessageID: "same", Files: []FileAttachment{{Data: []byte("two"), FileName: "same.txt"}}}, 10)
	firstDir := filepath.Dir(first.Entries()[0].Path)
	secondDir := filepath.Dir(second.Entries()[0].Path)
	if firstDir == secondDir {
		t.Fatalf("same message id reused staging dir %q", firstDir)
	}
	if !strings.HasPrefix(filepath.Base(firstDir), "same-") || !strings.HasPrefix(filepath.Base(secondDir), "same-") {
		t.Fatalf("staging dirs should include safe id prefix plus nonce: %q %q", firstDir, secondDir)
	}
}

func TestStagedAttachmentsEntriesDefensiveCopy(t *testing.T) {
	// Catches Entries implementations that return mutable internal metadata.
	staged := mustStage(t, t.TempDir(), &Message{MessageID: "copy", Files: []FileAttachment{{Data: []byte("data"), FileName: "a.txt"}}}, 10)
	entries := staged.Entries()
	entries[0].Path = "/tmp/attacker"
	entries[0].Kind = "file"
	fresh := staged.Entries()
	if fresh[0].Path == "/tmp/attacker" {
		t.Fatalf("Entries returned mutable internal slice")
	}
}

func TestStagedAttachmentsReadAllVerifiesIdentityHashAndDoesNotCacheBytes(t *testing.T) {
	// Catches readers that trust paths after staging or cache old bytes across reads.
	workDir := t.TempDir()
	staged := mustStage(t, workDir, &Message{MessageID: "read", Images: []ImageAttachment{{MimeType: "image/png", Data: []byte("good"), FileName: "a.png"}}}, 10)
	images, files, err := staged.ReadAll()
	if err != nil {
		t.Fatalf("ReadAll() first error = %v", err)
	}
	if len(images) != 1 || len(files) != 0 || string(images[0].Data) != "good" {
		t.Fatalf("ReadAll() first = images=%v files=%v", images, files)
	}
	entry := staged.Entries()[0]
	if err := os.WriteFile(entry.Path, []byte("evil"), 0o644); err != nil {
		t.Fatalf("same inode rewrite fixture: %v", err)
	}
	_, _, err = staged.ReadAll()
	if got := stagedReason(t, err); got != "attachment_write_failed" {
		t.Fatalf("same inode same-size rewrite reason = %q, want attachment_write_failed (err=%v)", got, err)
	}
}

func TestStagedAttachmentsReadAllRejectsFinalSymlinkAndNonRegularFiles(t *testing.T) {
	// Catches readers that open the path without a final no-follow / regular-file check.
	for _, tc := range []struct {
		name    string
		replace func(path string) error
	}{
		{"final symlink", func(path string) error {
			if err := os.Remove(path); err != nil {
				return err
			}
			return os.Symlink("/etc/passwd", path)
		}},
		{"fifo", func(path string) error {
			if runtime.GOOS == "windows" {
				return nil
			}
			if err := os.Remove(path); err != nil {
				return err
			}
			return syscall.Mkfifo(path, 0o644)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			staged := mustStage(t, t.TempDir(), &Message{MessageID: tc.name, Files: []FileAttachment{{Data: []byte("data"), FileName: "a.txt"}}}, 10)
			if err := tc.replace(staged.Entries()[0].Path); err != nil {
				t.Fatalf("replace fixture: %v", err)
			}
			_, _, err := staged.ReadAll()
			if got := stagedReason(t, err); got != "attachment_write_failed" {
				t.Fatalf("ReadAll reason = %q, want attachment_write_failed (err=%v)", got, err)
			}
		})
	}
}

func TestStageMessageAttachmentsCleansPartialFilesOnWriteFailure(t *testing.T) {
	// Catches non-transactional staging that leaves the first file behind when a
	// later write/fsync/close operation fails.
	workDir := t.TempDir()
	msg := &Message{MessageID: "partial", Files: []FileAttachment{
		{Data: []byte("first"), FileName: "first.txt"},
		{Data: []byte("second"), FileName: "CANARY-second.txt"},
	}}
	ops := defaultStagedFileOps()
	ops.afterWrite = func(index int, _ string, _ *os.File) error {
		if index == 1 {
			return errors.New("CANARY raw disk failure")
		}
		return nil
	}
	staged, err := stageMessageAttachmentsWithOps(workDir, msg, 10, ops)
	if got := stagedReason(t, err); got != "attachment_write_failed" {
		t.Fatalf("reason = %q, want attachment_write_failed (err=%v)", got, err)
	}
	if staged != nil || msg.Staged != nil || len(msg.Files) != 2 {
		t.Fatalf("failed stage mutated ownership: staged=%v msg.Staged=%v files=%d", staged, msg.Staged, len(msg.Files))
	}
	if strings.Contains(err.Error(), "CANARY") {
		t.Fatalf("safe error leaked raw write cause: %v", err)
	}
	attachmentsRoot := filepath.Join(workDir, ".cc-connect", "attachments")
	entries, readErr := os.ReadDir(attachmentsRoot)
	if readErr == nil && len(entries) != 0 {
		t.Fatalf("partial failure left staged entries under %q: %v", attachmentsRoot, entries)
	}
}

func TestStageMessageAttachmentsCleansPartialFilesOnFsyncAndCloseFailure(t *testing.T) {
	// Catches staging that treats fsync/close failures as success or leaves prior
	// files behind when late durability steps fail.
	for _, tc := range []struct {
		name string
		ops  func(stagedFileOps) stagedFileOps
	}{
		{"fsync failure", func(ops stagedFileOps) stagedFileOps {
			ops.syncFile = func(index int, _ *os.File) error {
				if index == 1 {
					return errors.New("CANARY raw fsync failure")
				}
				return nil
			}
			return ops
		}},
		{"close failure", func(ops stagedFileOps) stagedFileOps {
			ops.closeFile = func(index int, _ *os.File) error {
				if index == 1 {
					return errors.New("CANARY raw close failure")
				}
				return nil
			}
			return ops
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workDir := t.TempDir()
			msg := &Message{MessageID: tc.name, Files: []FileAttachment{
				{Data: []byte("first"), FileName: "first.txt"},
				{Data: []byte("second"), FileName: "second.txt"},
			}}
			staged, err := stageMessageAttachmentsWithOps(workDir, msg, 10, tc.ops(defaultStagedFileOps()))
			if got := stagedReason(t, err); got != "attachment_write_failed" {
				t.Fatalf("reason = %q, want attachment_write_failed (err=%v)", got, err)
			}
			if staged != nil || msg.Staged != nil || len(msg.Files) != 2 {
				t.Fatalf("failed stage mutated ownership: staged=%v msg.Staged=%v files=%d", staged, msg.Staged, len(msg.Files))
			}
			if strings.Contains(err.Error(), "CANARY") {
				t.Fatalf("safe error leaked raw cause: %v", err)
			}
			entries, readErr := os.ReadDir(filepath.Join(workDir, ".cc-connect", "attachments"))
			if readErr == nil && len(entries) != 0 {
				t.Fatalf("late failure left staged entries: %v", entries)
			}
		})
	}
}

func TestStageMessageAttachmentsRejectsShortWriteWithoutRawLeak(t *testing.T) {
	// Catches single Write implementations that ignore n < len(data) with nil err.
	ops := defaultStagedFileOps()
	ops.writeFile = func(_ int, _ *os.File, data []byte) (int, error) {
		return len(data) - 1, nil
	}
	msg := &Message{MessageID: "short", Files: []FileAttachment{{Data: []byte("payload"), FileName: "CANARY-short.txt"}}}
	staged, err := stageMessageAttachmentsWithOps(t.TempDir(), msg, 20, ops)
	if got := stagedReason(t, err); got != "attachment_write_failed" {
		t.Fatalf("reason = %q, want attachment_write_failed (staged=%v err=%v)", got, staged, err)
	}
	if strings.Contains(err.Error(), "CANARY") {
		t.Fatalf("safe error leaked filename: %v", err)
	}
}

func TestStagedAttachmentsReadAllRejectsRegularReplacement(t *testing.T) {
	// Catches readers that only check path stays under root while accepting a
	// different regular file or hardlink substituted after staging.
	for _, tc := range []struct {
		name    string
		replace func(path string) error
	}{
		{"regular file", func(path string) error {
			if err := os.Remove(path); err != nil {
				return err
			}
			return os.WriteFile(path, []byte("evil"), 0o644)
		}},
		{"hardlink", func(path string) error {
			donor := filepath.Join(filepath.Dir(path), "donor")
			if err := os.WriteFile(donor, []byte("evil"), 0o644); err != nil {
				return err
			}
			if err := os.Remove(path); err != nil {
				return err
			}
			return os.Link(donor, path)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			staged := mustStage(t, t.TempDir(), &Message{MessageID: tc.name, Files: []FileAttachment{{Data: []byte("data"), FileName: "a.txt"}}}, 10)
			if err := tc.replace(staged.Entries()[0].Path); err != nil {
				t.Fatalf("replace fixture: %v", err)
			}
			_, _, err := staged.ReadAll()
			if got := stagedReason(t, err); got != "attachment_write_failed" {
				t.Fatalf("ReadAll reason = %q, want attachment_write_failed (err=%v)", got, err)
			}
		})
	}
}

func TestStageMessageAttachmentsDoesNotDeleteReplacementAncestorOnCreateFailure(t *testing.T) {
	// Catches production-equivalent failure cleanup that removes a replacement
	// ancestor after this process created the original component in a writable
	// workspace parent. The test seam only opens the window; it must not influence
	// cleanup safety decisions.
	workDir := t.TempDir()
	var sentinel string
	ops := defaultStagedFileOps()
	ops.afterMkdirBeforeValidate = func(component string) error {
		if component != ".cc-connect" {
			return nil
		}
		if err := os.Rename(filepath.Join(workDir, ".cc-connect"), filepath.Join(workDir, ".cc-connect-owned-detached")); err != nil {
			return err
		}
		replacement := filepath.Join(workDir, ".cc-connect")
		if err := os.Mkdir(replacement, 0o777); err != nil {
			return err
		}
		if err := os.Chmod(replacement, 0o777); err != nil {
			return err
		}
		sentinel = filepath.Join(replacement, "sentinel")
		return os.WriteFile(sentinel, []byte("keep"), 0o644)
	}
	msg := &Message{MessageID: "msg", Files: []FileAttachment{{Data: []byte("payload"), FileName: "payload.txt"}}}
	staged, err := stageMessageAttachmentsWithOps(workDir, msg, 20, ops)
	if got := stagedReason(t, err); got != "attachment_write_failed" {
		t.Fatalf("reason = %q, want attachment_write_failed (staged=%v err=%v)", got, staged, err)
	}
	if data, readErr := os.ReadFile(sentinel); readErr != nil || string(data) != "keep" {
		t.Fatalf("replacement sentinel deleted or changed: data=%q err=%v", data, readErr)
	}
}

func TestStageMessageAttachmentsRejectsExistingMessageNonceDirectory(t *testing.T) {
	// Catches nonce-collision handling that reuses an existing message directory
	// and therefore lets two staged objects share ownership of the same root.
	workDir := t.TempDir()
	parent := filepath.Join(workDir, ".cc-connect", "attachments")
	collisionDir := filepath.Join(parent, "msg-fixed")
	if err := os.MkdirAll(collisionDir, 0o755); err != nil {
		t.Fatalf("mkdir collision dir: %v", err)
	}
	sentinel := filepath.Join(collisionDir, "sentinel")
	if err := os.WriteFile(sentinel, []byte("keep"), 0o644); err != nil {
		t.Fatalf("write sentinel: %v", err)
	}
	ops := defaultStagedFileOps()
	ops.fixedNonce = "fixed"
	msg := &Message{MessageID: "msg", Files: []FileAttachment{{Data: []byte("payload"), FileName: "payload.txt"}}}
	staged, err := stageMessageAttachmentsWithOps(workDir, msg, 20, ops)
	if got := stagedReason(t, err); got != "attachment_write_failed" {
		t.Fatalf("reason = %q, want attachment_write_failed (staged=%v err=%v)", got, staged, err)
	}
	if data, readErr := os.ReadFile(sentinel); readErr != nil || string(data) != "keep" {
		t.Fatalf("collision sentinel deleted or changed: data=%q err=%v", data, readErr)
	}
	if _, statErr := os.Stat(filepath.Join(collisionDir, "payload.txt")); !os.IsNotExist(statErr) {
		t.Fatalf("collision directory was reused or overwritten; payload stat err=%v", statErr)
	}
}

func TestStageMessageAttachmentsFailsWhenTrustedRootIsReplacedAfterValidation(t *testing.T) {
	// Catches the exact window where a path component passes owner/mode Lstat
	// validation, then the workspace owner renames it before OpenRoot binds the fd.
	for _, tc := range []struct {
		name      string
		component string
		replace   func(workDir string, dirName string) (sentinel string, err error)
	}{
		{"cc-connect replaced by attacker dir", ".cc-connect", func(workDir, _ string) (string, error) {
			old := filepath.Join(workDir, ".cc-connect")
			if err := os.Rename(old, filepath.Join(workDir, ".cc-connect-detached")); err != nil {
				return "", err
			}
			attacker := filepath.Join(workDir, ".cc-connect", "attachments")
			if err := os.MkdirAll(attacker, 0o755); err != nil {
				return "", err
			}
			sentinel := filepath.Join(attacker, "sentinel")
			return sentinel, os.WriteFile(sentinel, []byte("keep"), 0o644)
		}},
		{"cc-connect replaced by relative symlink", ".cc-connect", func(workDir, _ string) (string, error) {
			outside := filepath.Join(workDir, "outside")
			if err := os.MkdirAll(filepath.Join(outside, "attachments"), 0o755); err != nil {
				return "", err
			}
			if err := os.Rename(filepath.Join(workDir, ".cc-connect"), filepath.Join(workDir, ".cc-connect-detached")); err != nil {
				return "", err
			}
			if err := os.Symlink("outside", filepath.Join(workDir, ".cc-connect")); err != nil {
				return "", err
			}
			sentinel := filepath.Join(outside, "attachments", "sentinel")
			return sentinel, os.WriteFile(sentinel, []byte("keep"), 0o644)
		}},
		{"attachments replaced by attacker dir", "attachments", func(workDir, _ string) (string, error) {
			base := filepath.Join(workDir, ".cc-connect")
			if err := os.Rename(filepath.Join(base, "attachments"), filepath.Join(base, "attachments-detached")); err != nil {
				return "", err
			}
			attacker := filepath.Join(base, "attachments")
			if err := os.MkdirAll(attacker, 0o755); err != nil {
				return "", err
			}
			sentinel := filepath.Join(attacker, "sentinel")
			return sentinel, os.WriteFile(sentinel, []byte("keep"), 0o644)
		}},
		{"message dir replaced before open", "message", func(workDir, dirName string) (string, error) {
			parent := filepath.Join(workDir, ".cc-connect", "attachments")
			if err := os.Rename(filepath.Join(parent, dirName), filepath.Join(parent, dirName+"-detached")); err != nil {
				return "", err
			}
			replacement := filepath.Join(parent, dirName)
			if err := os.MkdirAll(replacement, 0o755); err != nil {
				return "", err
			}
			sentinel := filepath.Join(replacement, "sentinel")
			return sentinel, os.WriteFile(sentinel, []byte("keep"), 0o644)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workDir := t.TempDir()
			var sentinel string
			ops := defaultStagedFileOps()
			ops.fixedNonce = "validated-race"
			ops.afterValidateBeforeOpen = func(component string) error {
				if component != tc.component {
					return nil
				}
				var err error
				sentinel, err = tc.replace(workDir, "msg-validated-race")
				return err
			}
			msg := &Message{MessageID: "msg", Files: []FileAttachment{{Data: []byte("payload"), FileName: "payload.txt"}}}
			staged, err := stageMessageAttachmentsWithOps(workDir, msg, 20, ops)
			if got := stagedReason(t, err); got != "attachment_write_failed" {
				t.Fatalf("reason = %q, want attachment_write_failed (staged=%v err=%v)", got, staged, err)
			}
			if sentinel != "" {
				if data, readErr := os.ReadFile(sentinel); readErr != nil || string(data) != "keep" {
					t.Fatalf("sentinel changed/deleted: data=%q err=%v", data, readErr)
				}
			}
		})
	}
}

func TestStageMessageAttachmentsWritesThroughHeldDirectoryWhenPathIsReplaced(t *testing.T) {
	// Catches implementations that create an owned message dir and then use an
	// absolute path for file creation, allowing a writable ancestor replacement to
	// redirect wxbridge writes outside the owned directory.
	for _, tc := range []struct {
		name    string
		replace func(workDir string) (outsideFile string, err error)
	}{
		{"root internal attacker dir", func(workDir string) (string, error) {
			old := filepath.Join(workDir, ".cc-connect")
			if err := os.Rename(old, filepath.Join(workDir, ".cc-connect-owned-detached")); err != nil {
				return "", err
			}
			attacker := filepath.Join(workDir, ".cc-connect", "attachments")
			if err := os.MkdirAll(attacker, 0o777); err != nil {
				return "", err
			}
			return filepath.Join(attacker, "race", "payload.txt"), nil
		}},
		{"root external symlink", func(workDir string) (string, error) {
			old := filepath.Join(workDir, ".cc-connect")
			outside := filepath.Join(workDir, "outside-target")
			if err := os.MkdirAll(filepath.Join(outside, "attachments", "race"), 0o777); err != nil {
				return "", err
			}
			if err := os.Rename(old, filepath.Join(workDir, ".cc-connect-owned-detached")); err != nil {
				return "", err
			}
			if err := os.Symlink(outside, old); err != nil {
				return "", err
			}
			return filepath.Join(outside, "attachments", "race", "payload.txt"), nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workDir := t.TempDir()
			var outside string
			ops := defaultStagedFileOps()
			ops.fixedNonce = "race"
			ops.beforeCreateFile = func(_ int, _ string) error {
				var err error
				outside, err = tc.replace(workDir)
				return err
			}
			msg := &Message{MessageID: "msg", Files: []FileAttachment{{Data: []byte("payload"), FileName: "payload.txt"}}}
			staged, err := stageMessageAttachmentsWithOps(workDir, msg, 20, ops)
			if err != nil {
				t.Fatalf("StageMessageAttachments() error = %v", err)
			}
			defer staged.Cleanup()
			if _, err := os.Stat(outside); !os.IsNotExist(err) {
				t.Fatalf("stage wrote through replaced absolute path %q err=%v", outside, err)
			}
			_, files, err := staged.ReadAll()
			if err != nil || len(files) != 1 || string(files[0].Data) != "payload" {
				t.Fatalf("fd-bound ReadAll after path replacement files=%v err=%v", files, err)
			}
		})
	}
}

func TestStageMessageAttachmentsRejectsUnsafeExistingAttachmentAncestors(t *testing.T) {
	// Catches reuse of clagent/attacker-owned or group-writable staging ancestors.
	for _, tc := range []struct {
		name string
		mode os.FileMode
	}{
		{"group writable cc-connect", 0o775},
		{"other writable cc-connect", 0o777},
	} {
		t.Run(tc.name, func(t *testing.T) {
			workDir := t.TempDir()
			if err := os.Mkdir(filepath.Join(workDir, ".cc-connect"), tc.mode); err != nil {
				t.Fatalf("mkdir fixture: %v", err)
			}
			if err := os.Chmod(filepath.Join(workDir, ".cc-connect"), tc.mode); err != nil {
				t.Fatalf("chmod fixture: %v", err)
			}
			msg := &Message{MessageID: "unsafe", Files: []FileAttachment{{Data: []byte("data"), FileName: "a.txt"}}}
			staged, err := StageMessageAttachments(workDir, msg, 10)
			if got := stagedReason(t, err); got != "attachment_write_failed" {
				t.Fatalf("reason = %q, want attachment_write_failed (staged=%v err=%v)", got, staged, err)
			}
		})
	}
}

func TestStagedAttachmentsCleanupUsesHeldParentWhenPathIsReplacedAfterCheck(t *testing.T) {
	// Catches check-then-absolute-RemoveAll cleanup that can delete a replacement
	// directory created in the same path after identity validation.
	workDir := t.TempDir()
	ops := defaultStagedFileOps()
	ops.fixedNonce = "cleanup-race"
	msg := &Message{MessageID: "msg", Files: []FileAttachment{{Data: []byte("data"), FileName: "a.txt"}}}
	staged, err := stageMessageAttachmentsWithOps(workDir, msg, 10, ops)
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	dir := filepath.Dir(staged.Entries()[0].Path)
	marker := filepath.Join(dir, "sentinel")
	staged.cleanupBeforeRemove = func() error {
		if err := os.Rename(dir, filepath.Join(workDir, "detached-owned-dir")); err != nil {
			return err
		}
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		return os.WriteFile(marker, []byte("keep"), 0o644)
	}
	if err := staged.Cleanup(); err == nil {
		t.Fatalf("Cleanup() succeeded after replacement race, want safe failure")
	}
	if data, err := os.ReadFile(marker); err != nil || string(data) != "keep" {
		t.Fatalf("replacement sentinel was deleted or changed: data=%q err=%v", data, err)
	}
}

func TestStagedAttachmentsCleanupIsConcurrentOnceAndDoesNotDeleteReplacementDirectory(t *testing.T) {
	// Catches Cleanup implementations that call RemoveAll blindly or run the
	// underlying delete path more than once under concurrent terminal races.
	staged := mustStage(t, t.TempDir(), &Message{MessageID: "cleanup", Files: []FileAttachment{{Data: []byte("data"), FileName: "a.txt"}}}, 10)
	dir := filepath.Dir(staged.Entries()[0].Path)

	var wg sync.WaitGroup
	errs := make(chan error, 100)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs <- staged.Cleanup()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Cleanup() error = %v", err)
		}
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("cleanup dir stat err = %v, want not exist", err)
	}

	staged = mustStage(t, t.TempDir(), &Message{MessageID: "replace", Files: []FileAttachment{{Data: []byte("data"), FileName: "a.txt"}}}, 10)
	dir = filepath.Dir(staged.Entries()[0].Path)
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("remove owned dir fixture: %v", err)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("mkdir replacement fixture: %v", err)
	}
	marker := filepath.Join(dir, "replacement-marker")
	if err := os.WriteFile(marker, []byte("keep"), 0o644); err != nil {
		t.Fatalf("write replacement marker: %v", err)
	}
	if err := staged.Cleanup(); err == nil {
		t.Fatalf("Cleanup() accepted replacement directory")
	}
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("cleanup deleted replacement marker: %v", err)
	}
}
