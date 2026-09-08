package config

import "fmt"

// joinPath 組合設定 object path；root path 不應產生多餘的前導句點。
func joinPath(parent, key string) string {
	if parent == "" {
		return key
	}
	return parent + "." + key
}

// indexPath 將 slice/array index 加到設定 path，保留 root element 的 [n] 形式。
func indexPath(parent string, index int) string {
	return fmt.Sprintf("%s[%d]", parent, index)
}
