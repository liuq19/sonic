//go:build go1.24
// +build go1.24

package rt

import (
	"unsafe"
)

// Go 1.24 and later use the SwissMap iterator layout by default.
type GoMapIterator struct {
	K  unsafe.Pointer
	V  unsafe.Pointer
	T  *GoMapType
	It unsafe.Pointer
}
