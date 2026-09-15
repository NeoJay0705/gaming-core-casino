package localmq

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestInitStoragePublishesStableRootControl(t *testing.T) {
	root := t.TempDir()
	request := InitStorageRequest{
		RootPath:              root,
		StorageID:             "01993e11-8d3a-7c8f-a7ce-2ab21df8d410",
		ProducerStopFreeBytes: 1 << 20,
		RecoveryReserveBytes:  1 << 19,
		MaxClockSkew:          time.Second,
	}
	if err := InitStorage(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	if err := InitStorage(context.Background(), request); err != nil {
		t.Fatal(err)
	}
	var control storageControl
	if err := readMetadata(filepath.Join(root, storageVersionDirectory, storageControlFile), metadataKindStorageControl, &control); err != nil {
		t.Fatal(err)
	}
	if control.StorageID != request.StorageID || control.ProducerStopFreeBytes != request.ProducerStopFreeBytes || control.RecoveryReserveBytes != request.RecoveryReserveBytes {
		t.Fatalf("storage control = %+v", control)
	}
}
