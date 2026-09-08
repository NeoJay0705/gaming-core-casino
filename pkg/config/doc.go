// Package config 提供設定 layer 載入、canonical tree merge、immutable snapshot
// 與 typed struct binding。產品 input loader 另外提供 named source 與檔案完整性
// artifact；設定 struct 欄位只使用 config tag，json/yaml/xml tag 不參與 Bind。
package config
