package config

import (
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func canonicalMap(value map[string]any) (map[string]any, error) {
	canonical, err := canonicalizeValue(value, "", map[uintptr]bool{})
	if err != nil {
		return nil, err
	}
	return canonical.(map[string]any), nil
}

// canonicalizeValue 轉換並深拷貝 Layer output，建立 Snapshot 可安全持有的 tree。
func canonicalizeValue(value any, path string, seen map[uintptr]bool) (any, error) {
	if value == nil {
		return nil, nil
	}
	switch typed := value.(type) {
	case json.Number:
		text := string(typed)
		if strings.ContainsAny(text, ".eE") {
			parsed, err := strconv.ParseFloat(text, 64)
			if err != nil || math.IsInf(parsed, 0) || math.IsNaN(parsed) {
				return nil, fmt.Errorf("invalid number at %q", path)
			}
			return parsed, nil
		}
		if parsed, err := strconv.ParseInt(text, 10, 64); err == nil {
			return parsed, nil
		}
		if parsed, err := strconv.ParseUint(text, 10, 64); err == nil {
			return parsed, nil
		}
		return nil, fmt.Errorf("invalid number at %q", path)
	case map[string]any:
		canonical := make(map[string]any, len(typed))
		pointer := reflect.ValueOf(typed).Pointer()
		if seen[pointer] {
			return nil, fmt.Errorf("cycle at %q", path)
		}
		seen[pointer] = true
		defer delete(seen, pointer)
		for key, child := range typed {
			converted, err := canonicalizeValue(child, joinPath(path, key), seen)
			if err != nil {
				return nil, err
			}
			canonical[key] = converted
		}
		return canonical, nil
	case []any:
		pointer := reflect.ValueOf(typed).Pointer()
		if pointer != 0 {
			if seen[pointer] {
				return nil, fmt.Errorf("cycle at %q", path)
			}
			seen[pointer] = true
			defer delete(seen, pointer)
		}
		canonical := make([]any, len(typed))
		for index, child := range typed {
			converted, err := canonicalizeValue(child, indexPath(path, index), seen)
			if err != nil {
				return nil, err
			}
			canonical[index] = converted
		}
		return canonical, nil
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case uint:
		return uint64(typed), nil
	case uint8:
		return uint64(typed), nil
	case uint16:
		return uint64(typed), nil
	case uint32:
		return uint64(typed), nil
	case float32:
		parsed := float64(typed)
		if math.IsNaN(parsed) || math.IsInf(parsed, 0) {
			return nil, fmt.Errorf("invalid float at %q", path)
		}
		return parsed, nil
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return nil, fmt.Errorf("invalid float at %q", path)
		}
		return typed, nil
	case string, bool, int64, uint64:
		return typed, nil
	default:
		return nil, fmt.Errorf("unsupported value at %q", path)
	}
}

// cloneCanonicalValue 複製已通過 canonicalization 的 map/slice branch。
func cloneCanonicalValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		cloned := make(map[string]any, len(typed))
		for key, child := range typed {
			cloned[key] = cloneCanonicalValue(child)
		}
		return cloned
	case []any:
		cloned := make([]any, len(typed))
		for index, child := range typed {
			cloned[index] = cloneCanonicalValue(child)
		}
		return cloned
	default:
		return typed
	}
}

// mergeInto 將 source 合併到 caller-owned destination；source branch 會複製，
// 避免 shared document 或 custom Layer output 被後續 merge 修改。
func mergeInto(destination, source map[string]any, path string) error {
	for key, value := range source {
		currentPath := joinPath(path, key)
		old, exists := destination[key]
		if !exists {
			destination[key] = cloneCanonicalValue(value)
			continue
		}

		oldMap, oldIsMap := old.(map[string]any)
		newMap, newIsMap := value.(map[string]any)
		if oldIsMap != newIsMap {
			return fmt.Errorf("type conflict at %q: %s vs %s", currentPath, valueKind(old), valueKind(value))
		}
		if oldIsMap {
			if err := mergeInto(oldMap, newMap, currentPath); err != nil {
				return err
			}
			continue
		}
		destination[key] = cloneCanonicalValue(value)
	}
	return nil
}

func valueKind(value any) string {
	switch value.(type) {
	case map[string]any:
		return "map"
	case []any:
		return "slice"
	case nil:
		return "null"
	default:
		return reflect.TypeOf(value).Kind().String()
	}
}
