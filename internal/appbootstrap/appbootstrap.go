// Package appbootstrap 提供 SDK-owned 服務 App 共用的設定 bootstrap。
//
// 本 package 刻意停在服務 module 組裝之前：GameSvr、GateSvr 與 GMS 的依賴圖不同，
// 不把差異藏進 callback，才能讓各服務的 NewApp 保持直線且可讀。
package appbootstrap

import (
	"context"
	"fmt"
	"strings"

	"github.com/NeoJay0705/gaming-core-casino/pkg/config"
)

// Load 只讀取一次設定文件並保留 merged tree 與 named source view。
//
// context、input 與 environment 驗證由 config.LoadInputsWithArtifacts 負責；本函式只補上
// bootstrap 階段標記並以 %w 保留底層錯誤，讓呼叫端能辨識失敗階段。各 product
// module 自行把 Snapshot 投影成它需要的 typed config，避免 App flow 知道圖內 provider。
func Load(ctx context.Context, inputs config.ConfigInputs, envPrefix string) (config.SourceSnapshot, error) {
	loaded, err := LoadWithArtifacts(ctx, inputs, envPrefix)
	if err != nil {
		return nil, err
	}
	return loaded.Snapshot, nil
}

// LoadWithArtifacts 載入與 Load 相同的 snapshot，並額外回傳 named source
// 的檔案資訊。錯誤 stage 仍在此處包裝，caller 不需要知道 config 內部實作。
func LoadWithArtifacts(ctx context.Context, inputs config.ConfigInputs, envPrefix string) (config.LoadedInputs, error) {
	loaded, err := config.LoadInputsWithArtifacts(ctx, inputs, envPrefix)
	if err != nil {
		return config.LoadedInputs{}, wrapConfigInputsError(err)
	}
	return loaded, nil
}

// wrapConfigInputsError 只在底層尚未提供 stage marker 時補上前綴，避免錯誤訊息
// 變成「load config inputs: load config inputs: ...」的重複包裝。
func wrapConfigInputsError(err error) error {
	const prefix = "load config inputs:"
	if strings.HasPrefix(err.Error(), prefix) {
		return err
	}
	return fmt.Errorf("%s %w", prefix, err)
}
