package framework

import (
	"fmt"
	"testing"
)

type customError struct{}

func (c *customError) Error() string { return "custom error" }

// TestIsNilDependency 測試 IsNilDependency 在各種 Typed Nil 與有效值情境下的判定。
//
// 測試涵蓋：
//   - Untyped Nil (`nil`)
//   - Typed Nil Pointer (`*string(nil)`)
//   - Typed Nil Interface (`error((*customError)(nil))`)
//   - Typed Nil Func (`func()(nil)`)
//   - Typed Nil Chan (`chan int(nil)`)
//   - Typed Nil Map (`map[string]int(nil)`)
//   - Typed Nil Slice (`[]int(nil)`)
//   - 有效實體（Pointer, Func, Chan, Map, Slice, Primitive Types, Struct）
func TestIsNilDependency(t *testing.T) {
	var (
		untypedNil    any
		typedNilPtr   *string
		typedNilErr   error = (*customError)(nil)
		typedNilFunc  func()
		typedNilChan  chan int
		typedNilMap   map[string]int
		typedNilSlice []int

		validPtr    = new(string)
		validFunc   = func() {}
		validChan   = make(chan int)
		validMap    = make(map[string]int)
		validSlice  = make([]int, 0)
		validInt    = 42
		validStr    = "hello"
		validStruct = struct{}{}
	)

	tests := []struct {
		name     string
		input    any
		expected bool
	}{
		{"untyped nil", untypedNil, true},
		{"typed nil pointer", typedNilPtr, true},
		{"typed nil error interface", typedNilErr, true},
		{"typed nil func", typedNilFunc, true},
		{"typed nil chan", typedNilChan, true},
		{"typed nil map", typedNilMap, true},
		{"typed nil slice", typedNilSlice, true},
		{"valid pointer", validPtr, false},
		{"valid func", validFunc, false},
		{"valid chan", validChan, false},
		{"valid map", validMap, false},
		{"valid slice", validSlice, false},
		{"valid int", validInt, false},
		{"valid string", validStr, false},
		{"valid struct", validStruct, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsNilDependency(tt.input)
			if got != tt.expected {
				t.Errorf("IsNilDependency(%s) = %v, want %v", tt.name, got, tt.expected)
			}
		})
	}
}

func ExampleIsNilDependency() {
	var fn func()
	fmt.Println(IsNilDependency(fn))
	fn = func() {}
	fmt.Println(IsNilDependency(fn))
	// Output:
	// true
	// false
}
