package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithInt32 uintptr

func (u LKVTableObjectUPtrWithInt32) Ptr() *LKVTableObjectWithInt32 {
	return (*LKVTableObjectWithInt32)(unsafe.Pointer(u))
}

type LKVTableObjectWithInt32 struct {
	ID int32
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithInt32 struct {
	KVTableCommon
	Shards []map[int32]LKVTableObjectUPtrWithInt32

	ReleaseObjectPolicyIsNeedRelease bool
}

func (p *OffheapDriver) InitLKVTableWithInt32(kvTable *LKVTableWithInt32, name string,
	objectSize int, objectsLimit int32, shardCount uint32,
	beforeReleaseObjectFunc KVTableInvokeBeforeReleaseObject,
) error {
	var (
		err error
	)
	err = kvTable.Init(name, objectSize, objectsLimit, shardCount,
		beforeReleaseObjectFunc,
	)
	if err != nil {
		return err
	}

	return err
}

func (p *LKVTableWithInt32) Init(name string,
	objectSize int, objectsLimit int32, shardCount uint32,
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

	p.beforeReleaseObjectFunc = beforeReleaseObjectFunc

	p.ReleaseObjectPolicyIsNeedRelease = false

	return nil
}

func (p *LKVTableWithInt32) Name() string {
	return p.name
}

func (p *LKVTableWithInt32) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[int32]LKVTableObjectUPtrWithInt32, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[int32]LKVTableObjectUPtrWithInt32)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectInt32)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithInt32) objectPoolInvokeReleaseObjectInt32() {
	var (
		shardIndex      uint32
		shard           *map[int32]LKVTableObjectUPtrWithInt32
		shardRWMutex    *sync.RWMutex
		objKey          int32
		uObject         LKVTableObjectUPtrWithInt32
		uReleaseTargetK int32
		uReleaseTarget  LKVTableObjectUPtrWithInt32
	)

	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		shard = &p.Shards[shardIndex]
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

func (p *LKVTableWithInt32) allocObjectWithInt32WithAcquire(objKey int32) LKVTableObjectUPtrWithInt32 {
	var uObject = LKVTableObjectUPtrWithInt32(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithInt32) TryGetObjectWithAcquire(objKey int32) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithInt32 = 0
		shard        *map[int32]LKVTableObjectUPtrWithInt32
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithInt32(objKey)
		shard = &p.Shards[shardIndex]
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

// MustGetObjectWithAcquire return uObject, loaded
func (p *LKVTableWithInt32) MustGetObjectWithAcquire(objKey int32) (uintptr, KVTableAfterSetNewObj) {
	var (
		uObject           LKVTableObjectUPtrWithInt32 = 0
		shard             *map[int32]LKVTableObjectUPtrWithInt32
		shardRWMutex      *sync.RWMutex
		isNewObjectSetted bool = false
	)

	{
		shardIndex := p.GetShardWithInt32(objKey)
		shard = &p.Shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.RLock()
	uObject, _ = (*shard)[objKey]
	if uObject != 0 {
		uObject.Ptr().Acquire()
	}
	shardRWMutex.RUnlock()

	if uObject != 0 {
		return uintptr(uObject), nil
	}

	shardRWMutex.Lock()
	uObject, _ = (*shard)[objKey]
	var afterSetObj KVTableAfterSetNewObj = func() {
		uObject.Ptr().Acquire()
		shardRWMutex.Unlock()
	}
	if uObject == 0 {
		uObject = p.allocObjectWithInt32WithAcquire(objKey)
		(*shard)[objKey] = uObject
		isNewObjectSetted = true
	}

	if isNewObjectSetted == false {
		afterSetObj()
		return uintptr(uObject), nil
	}

	return uintptr(uObject), afterSetObj
}

func (p *LKVTableWithInt32) DeleteObject(objKey int32) {
	var (
		uObject      LKVTableObjectUPtrWithInt32
		shard        *map[int32]LKVTableObjectUPtrWithInt32
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithInt32(objKey)
		shard = &p.Shards[shardIndex]
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

func (p *LKVTableWithInt32) ReleaseObject(uObject LKVTableObjectUPtrWithInt32) {
	var isShouldRelease = (uObject.Ptr().Release() == 0) && p.ReleaseObjectPolicyIsNeedRelease
	if isShouldRelease == false {
		return
	}

	var (
		shard        *map[int32]LKVTableObjectUPtrWithInt32
		shardRWMutex *sync.RWMutex
		objKey       = uObject.Ptr().ID
	)

	{
		shardIndex := p.GetShardWithInt32(objKey)
		shard = &p.Shards[shardIndex]
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
