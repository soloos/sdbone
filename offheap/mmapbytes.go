package offheap

import (
	"syscall"
	"unsafe"
)

type mmapbytes struct {
	addrStart uintptr
	addrEnd   uintptr
}

func AllocMmapBytes(size int) (mmapbytes, error) {
	var (
		ret   mmapbytes
		bytes []byte
		err   error
	)
	prot := syscall.PROT_READ | syscall.PROT_WRITE
	flags := syscall.MAP_ANON | syscall.MAP_PRIVATE

	bytes, err = syscall.Mmap(-1, 0, size, prot, flags)
	if err != nil {
		panic(err)
	}
	ret.addrStart = *((*uintptr)((unsafe.Pointer)(&bytes)))
	ret.addrEnd = ret.addrStart + uintptr(size)
	return ret, err
}
