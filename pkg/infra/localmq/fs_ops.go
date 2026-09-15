package localmq

import (
	"os"

	"golang.org/x/sys/unix"
)

// fileHandle 是 localmq 實際需要的最小檔案介面。Production 直接使用
// *os.File；測試可以在不建立 virtual filesystem 的前提下替換 storageOps。
type fileHandle interface {
	ReadAt([]byte, int64) (int, error)
	Write([]byte) (int, error)
	Seek(int64, int) (int64, error)
	Truncate(int64) error
	Chmod(os.FileMode) error
	Sync() error
	Stat() (os.FileInfo, error)
	Close() error
}

const (
	faultRecordAppend         = "record_append"
	faultWalSync              = "wal_sync"
	faultDurableEndTempSync   = "durable_end_temp_sync"
	faultDurableEndRename     = "durable_end_rename"
	faultDurableEndParentSync = "durable_end_parent_sync"
	faultFooterSync           = "footer_sync"
	faultSegmentRename        = "segment_rename"
	faultCheckpointRename     = "checkpoint_rename"
	faultRetentionFloorRename = "retention_floor_rename"
	faultStagingRename        = "staging_rename"
	faultGroupControlRename   = "group_control_rename"
)

// storageOps 只包裝 localmq 已使用的 filesystem primitives；不提供通用
// filesystem abstraction。fault 只供 package-private conformance tests 使用。
type storageOps struct {
	openFile   func(string, int, os.FileMode) (fileHandle, error)
	open       func(string) (fileHandle, error)
	createTemp func(string, string) (fileHandle, string, error)
	mkdirAll   func(string, os.FileMode) error
	mkdirTemp  func(string, string) (string, error)
	mkdir      func(string, os.FileMode) error
	readFile   func(string) ([]byte, error)
	readDir    func(string) ([]os.DirEntry, error)
	stat       func(string) (os.FileInfo, error)
	rename     func(string, string) error
	remove     func(string) error
	removeAll  func(string) error
	syncDir    func(string) error
	statfs     func(string) (uint64, error)
	fault      func(string) error
}

func newStorageOps() *storageOps {
	return &storageOps{
		openFile: func(path string, flag int, perm os.FileMode) (fileHandle, error) {
			return os.OpenFile(path, flag, perm)
		},
		open: func(path string) (fileHandle, error) {
			return os.Open(path)
		},
		createTemp: func(dir, pattern string) (fileHandle, string, error) {
			file, err := os.CreateTemp(dir, pattern)
			if err != nil {
				return nil, "", err
			}
			return file, file.Name(), nil
		},
		mkdirAll:  os.MkdirAll,
		mkdirTemp: os.MkdirTemp,
		mkdir:     os.Mkdir,
		readFile:  os.ReadFile,
		readDir:   os.ReadDir,
		stat:      os.Stat,
		rename:    os.Rename,
		remove:    os.Remove,
		removeAll: os.RemoveAll,
		syncDir:   syncDirectoryOS,
		statfs: func(path string) (uint64, error) {
			var stat unix.Statfs_t
			if err := unix.Statfs(path, &stat); err != nil {
				return 0, err
			}
			return uint64(stat.Bavail) * uint64(stat.Bsize), nil
		},
		fault: func(string) error { return nil },
	}
}

func (o *storageOps) fail(point string) error {
	if o == nil || o.fault == nil || point == "" {
		return nil
	}
	return o.fault(point)
}

func (s *storage) fsOps() *storageOps {
	if s.ops == nil {
		s.ops = newStorageOps()
	}
	return s.ops
}

func (s *storage) fault(point string) error { return s.fsOps().fail(point) }

func (s *storage) openFile(path string, flag int, perm os.FileMode) (fileHandle, error) {
	return s.fsOps().openFile(path, flag, perm)
}

func (s *storage) open(path string) (fileHandle, error) { return s.fsOps().open(path) }

func (s *storage) stat(path string) (os.FileInfo, error) { return s.fsOps().stat(path) }

func (s *storage) readFile(path string) ([]byte, error) { return s.fsOps().readFile(path) }

func (s *storage) readDir(path string) ([]os.DirEntry, error) { return s.fsOps().readDir(path) }

func (s *storage) mkdirAll(path string, perm os.FileMode) error {
	return s.fsOps().mkdirAll(path, perm)
}

func (s *storage) mkdirTemp(dir, pattern string) (string, error) {
	return s.fsOps().mkdirTemp(dir, pattern)
}

func (s *storage) mkdir(path string, perm os.FileMode) error { return s.fsOps().mkdir(path, perm) }

func (s *storage) rename(oldPath, newPath string) error { return s.fsOps().rename(oldPath, newPath) }

func (s *storage) remove(path string) error { return s.fsOps().remove(path) }

func (s *storage) removeAll(path string) error { return s.fsOps().removeAll(path) }

func (s *storage) syncDirectory(path string) error { return s.fsOps().syncDir(path) }

func syncDirectoryOS(path string) error {
	directory, err := os.Open(path)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}
