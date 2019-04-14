package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithBytes64 uintptr

func (u LKVTableObjectUPtrWithBytes64) Ptr() *LKVTableObjectWithBytes64 {
	return (*LKVTableObjectWithBytes64)(unsafe.Pointer(u))
}

type LKVTableObjectWithBytes64 struct {
	ID [64]byte
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithBytes64 struct {
	KVTableCommon
	shards []map[[64]byte]LKVTableObjectUPtrWithBytes64
}

func (p *OffheapDriver) InitLKVTableWithBytes64(kvTable *LKVTableWithBytes64, name string,
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

func (p *LKVTableWithBytes64) Init(name string,
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

func (p *LKVTableWithBytes64) Name() string {
	return p.name
}

func (p *LKVTableWithBytes64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.shards = make([]map[[64]byte]LKVTableObjectUPtrWithBytes64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.shards[shardIndex] = make(map[[64]byte]LKVTableObjectUPtrWithBytes64)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectBytes64)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithBytes64) objectPoolInvokeReleaseObjectBytes64() {
	var (
		shardIndex      uint32
		shard           *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex    *sync.RWMutex
		objKey          [64]byte
		uObject         LKVTableObjectUPtrWithBytes64
		uReleaseTargetK [64]byte
		uReleaseTarget  LKVTableObjectUPtrWithBytes64
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

func (p *LKVTableWithBytes64) allocObjectWithBytes64WithAcquire(objKey [64]byte) LKVTableObjectUPtrWithBytes64 {
	var uObject = LKVTableObjectUPtrWithBytes64(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithBytes64) TryGetObjectWithAcquire(objKey [64]byte) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithBytes64 = 0
		shard        *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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

func (p *LKVTableWithBytes64) MustGetObjectWithAcquire(objKey [64]byte) (uintptr, bool) {
	var (
		uObject      LKVTableObjectUPtrWithBytes64 = 0
		shard        *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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
		uObject = p.allocObjectWithBytes64WithAcquire(objKey)
		(*shard)[objKey] = uObject
		if p.prepareNewObjectFunc != nil {
			p.prepareNewObjectFunc(uintptr(uObject))
		}
	}
	uObject.Ptr().Acquire()
	shardRWMutex.Unlock()

	return uintptr(uObject), loaded
}

func (p *LKVTableWithBytes64) DeleteObject(objKey [64]byte) {
	var (
		uObject      LKVTableObjectUPtrWithBytes64
		shard        *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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

func (p *LKVTableWithBytes64) ReleaseObject(uObject LKVTableObjectUPtrWithBytes64) {
	var isShouldRelease = false
	if uObject.Ptr().Release() == 0 {
		isShouldRelease = true
	}

	if isShouldRelease == false {
		return
	}

	var (
		shard        *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
		objKey       = uObject.Ptr().ID
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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
