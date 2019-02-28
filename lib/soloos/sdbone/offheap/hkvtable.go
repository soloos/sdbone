package offheap

import (
	"sync"
)

type HKVTableInvokeBeforeReleaseObject func(v uintptr)
type HKVTableInvokeCheckObjectReleasable func(v uintptr)

// Heavy Key-Value table
type HKVTable struct {
	id           int64
	name         string
	keyType      string
	objectSize   int
	objectsLimit int32

	chunkPool        RawChunkPool
	sharedCount      uint32
	sharedRWMutexs   []sync.RWMutex
	sharedKeyType    string
	sharedsByString  []map[string]HKVTableObjectUPtrByString
	sharedsByInt32   []map[int32]HKVTableObjectUPtrByInt32
	sharedsByInt64   []map[int64]HKVTableObjectUPtrByInt64
	sharedsByBytes12 []map[[12]byte]HKVTableObjectUPtrByBytes12
	sharedsByBytes64 []map[[64]byte]HKVTableObjectUPtrByBytes64

	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject
}

func (p *OffheapDriver) CreateHKVTable(name string,
	objectSize int, objectsLimit int32,
	keyType string, sharedCount uint32,
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject,
) (*HKVTable, error) {
	var (
		kvTable = new(HKVTable)
		err     error
	)
	err = kvTable.Init(p.AllocTableID(), name, objectSize, objectsLimit, keyType, sharedCount,
		beforeReleaseObjectFunc,
	)
	if err != nil {
		return nil, err
	}

	return kvTable, err
}

func (p *HKVTable) Init(id int64, name string,
	objectSize int, objectsLimit int32,
	keyType string, sharedCount uint32,
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject,
) error {
	var err error

	p.id = id
	p.name = name
	p.keyType = keyType
	p.objectSize = objectSize
	p.objectsLimit = objectsLimit

	p.sharedCount = sharedCount
	p.sharedRWMutexs = make([]sync.RWMutex, p.sharedCount)

	err = p.prepareShareds(p.keyType, p.objectSize, p.objectsLimit)
	if err != nil {
		return err
	}

	p.beforeReleaseObjectFunc = beforeReleaseObjectFunc

	return nil
}

func (p *HKVTable) ID() int64 {
	return p.id
}

func (p *HKVTable) Name() string {
	return p.name
}
