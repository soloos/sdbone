package offheap

import "errors"

var (
	ErrUnknownKeyType       = errors.New("unknown keytype")
	ErrAllocChunkOurOfLimit = errors.New("alloc chunk out of limit")
	ErrMmap                 = errors.New("mmap error")
)
