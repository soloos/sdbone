package offheap

type OBytes struct {
	Data uintptr
	Len  int
	Cap  int
}

type OUintptrs struct {
	Data uintptr
	Len  int
	Cap  int
}
