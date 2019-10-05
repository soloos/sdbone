package offheap

import "errors"

var (
	ErrUnknownKeyType        = errors.New("unknown keytype")
	ErrAllocObjectOurOfLimit = errors.New("alloc object out of limit")
	ErrMmap                  = errors.New("mmap error")
)
