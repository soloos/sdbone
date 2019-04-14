package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithUint64 uintptr

func (u LKVTableObjectUPtrWithUint64) Ptr() *LKVTableObjectWithUint64 {
	return (*LKVTableObjectWithUint64)(unsafe.Pointer(u))
}

type LKVTableObjectWithUint64 struct {
	ID uint64
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithUint64 struct {
	KVTableCommon
	shards []map[uint64]LKVTableObjectUPtrWithUint64
}

func (p *OffheapDriver) InitLKVTableWithUint64(kvTable *LKVTableWithUint64, name string,
	objectSize int, objectsLimit int32, shardCount uint32,
	prepareNewObjectFunc KVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc KVTableInvokeBeforeReleaseObject,
) error {
	var (
		err error
	)
	err = kvTable.Init(name, objectSize, objectsLimit, shardCount,
		prepareNewObjectFunc,
		beforeReleaseObjectFunc,
	)
	if err != nil {
		return err
	}

	return err
}

func (p *LKVTableWithUint64) Init(name string,
	objectSize int, objectsLimit int32, shardCount uint32,
	prepareNewObjectFunc KVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc KVTableInvokeBeforeReleaseObject,
) error {
	var err error

	p.name = name
	p.objectSize = objectSize
	p.objectsLimit = objectsLimit

	p.shardCount = shardCount
	p.shardRWMutexs = make([]sync.RWMutex, p.shardCount)

	err = p.prepareShards(p.objectSize, p.objectsLimit)
	if err != nil {
		return err
	}

	p.prepareNewObjectFunc = prepareNewObjectFunc
	p.beforeReleaseObjectFunc = beforeReleaseObjectFunc

	return nil
}

func (p *LKVTableWithUint64) Name() string {
	return p.name
}

func (p *LKVTableWithUint64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.shards = make([]map[uint64]LKVTableObjectUPtrWithUint64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.shards[shardIndex] = make(map[uint64]LKVTableObjectUPtrWithUint64)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectUint64)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithUint64) objectPoolInvokeReleaseObjectUint64() {
	var (
		shardIndex      uint32
		shard           *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex    *sync.RWMutex
		objKey          uint64
		uObject         LKVTableObjectUPtrWithUint64
		uReleaseTargetK uint64
		uReleaseTarget  LKVTableObjectUPtrWithUint64
	)

	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		shard = &p.shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]

		shardRWMutex.RLock()
		for objKey, uObject = range *shard {
			if uObject.Ptr().GetAccessor() == 0 {
				uReleaseTargetK = objKey
				uReleaseTarget = uObject
				break
			}
		}
		shardRWMutex.RUnlock()
		if uReleaseTarget != 0 {
			goto FIND_TARGET_DONE
		}
	}

FIND_TARGET_DONE:
	if uReleaseTarget != 0 {
		p.DeleteObject(uReleaseTargetK)
	}
}

func (p *LKVTableWithUint64) allocObjectWithUint64WithAcquire(objKey uint64) LKVTableObjectUPtrWithUint64 {
	var uObject = LKVTableObjectUPtrWithUint64(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithUint64) TryGetObjectWithAcquire(objKey uint64) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithUint64 = 0
		shard        *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
		shard = &p.shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.RLock()
	uObject, _ = (*shard)[objKey]
	if uObject != 0 {
		uObject.Ptr().Acquire()
	}
	shardRWMutex.RUnlock()

	return uintptr(uObject)
}

func (p *LKVTableWithUint64) MustGetObjectWithAcquire(objKey uint64) (uintptr, bool) {
	var (
		uObject      LKVTableObjectUPtrWithUint64 = 0
		shard        *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
		shard = &p.shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.RLock()
	uObject, loaded = (*shard)[objKey]
	if uObject != 0 {
		uObject.Ptr().Acquire()
	}
	shardRWMutex.RUnlock()

	if uObject != 0 {
		return uintptr(uObject), loaded
	}

	shardRWMutex.Lock()
	uObject, loaded = (*shard)[objKey]
	if uObject == 0 {
		uObject = p.allocObjectWithUint64WithAcquire(objKey)
		(*shard)[objKey] = uObject
		if p.prepareNewObjectFunc != nil {
			p.prepareNewObjectFunc(uintptr(uObject))
		}
	}
	uObject.Ptr().Acquire()
	shardRWMutex.Unlock()

	return uintptr(uObject), loaded
}

func (p *LKVTableWithUint64) DeleteObject(objKey uint64) {
	var (
		uObject      LKVTableObjectUPtrWithUint64
		shard        *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
		shard = &p.shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.Lock()
	uObject, _ = (*shard)[objKey]
	if uObject != 0 && uObject.Ptr().GetAccessor() == 0 {
		if p.beforeReleaseObjectFunc != nil {
			p.beforeReleaseObjectFunc(uintptr(uObject))
		}
		delete(*shard, objKey)
		p.objectPool.ReleaseRawObject(uintptr(uObject))
	}
	shardRWMutex.Lock()
}

func (p *LKVTableWithUint64) ReleaseObject(uObject LKVTableObjectUPtrWithUint64) {
	var isShouldRelease = false
	if uObject.Ptr().Release() == 0 {
		isShouldRelease = true
	}

	if isShouldRelease == false {
		return
	}

	var (
		shard        *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
		objKey       = uObject.Ptr().ID
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
		shard = &p.shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.Lock()
	if uObject.Ptr().GetAccessor() == 0 {
		if p.beforeReleaseObjectFunc != nil {
			p.beforeReleaseObjectFunc(uintptr(uObject))
		}
		delete(*shard, objKey)
		p.objectPool.ReleaseRawObject(uintptr(uObject))
	}
	shardRWMutex.Unlock()
}