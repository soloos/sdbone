package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithInt64 uintptr

func (u LKVTableObjectUPtrWithInt64) Ptr() *LKVTableObjectWithInt64 {
	return (*LKVTableObjectWithInt64)(unsafe.Pointer(u))
}

type LKVTableObjectWithInt64 struct {
	ID int64
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithInt64 struct {
	KVTableCommon
	shards []map[int64]LKVTableObjectUPtrWithInt64
}

func (p *OffheapDriver) InitLKVTableWithInt64(kvTable *LKVTableWithInt64, name string,
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

func (p *LKVTableWithInt64) Init(name string,
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

func (p *LKVTableWithInt64) Name() string {
	return p.name
}

func (p *LKVTableWithInt64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.shards = make([]map[int64]LKVTableObjectUPtrWithInt64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.shards[shardIndex] = make(map[int64]LKVTableObjectUPtrWithInt64)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectInt64)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithInt64) objectPoolInvokeReleaseObjectInt64() {
	var (
		shardIndex      uint32
		shard           *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex    *sync.RWMutex
		objKey          int64
		uObject         LKVTableObjectUPtrWithInt64
		uReleaseTargetK int64
		uReleaseTarget  LKVTableObjectUPtrWithInt64
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

func (p *LKVTableWithInt64) allocObjectWithInt64WithAcquire(objKey int64) LKVTableObjectUPtrWithInt64 {
	var uObject = LKVTableObjectUPtrWithInt64(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithInt64) TryGetObjectWithAcquire(objKey int64) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithInt64 = 0
		shard        *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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

func (p *LKVTableWithInt64) MustGetObjectWithAcquire(objKey int64) (uintptr, bool) {
	var (
		uObject      LKVTableObjectUPtrWithInt64 = 0
		shard        *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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
		uObject = p.allocObjectWithInt64WithAcquire(objKey)
		(*shard)[objKey] = uObject
		if p.prepareNewObjectFunc != nil {
			p.prepareNewObjectFunc(uintptr(uObject))
		}
	}
	uObject.Ptr().Acquire()
	shardRWMutex.Unlock()

	return uintptr(uObject), loaded
}

func (p *LKVTableWithInt64) DeleteObject(objKey int64) {
	var (
		uObject      LKVTableObjectUPtrWithInt64
		shard        *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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

func (p *LKVTableWithInt64) ReleaseObject(uObject LKVTableObjectUPtrWithInt64) {
	var isShouldRelease = false
	if uObject.Ptr().Release() == 0 {
		isShouldRelease = true
	}

	if isShouldRelease == false {
		return
	}

	var (
		shard        *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
		objKey       = uObject.Ptr().ID
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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
