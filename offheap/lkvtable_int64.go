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
	Shards []map[int64]LKVTableObjectUPtrWithInt64

	ReleaseObjectPolicyIsNeedRelease bool
}

func (p *OffheapDriver) InitLKVTableWithInt64(kvTable *LKVTableWithInt64, name string,
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

func (p *LKVTableWithInt64) Init(name string,
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

func (p *LKVTableWithInt64) Name() string {
	return p.name
}

func (p *LKVTableWithInt64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[int64]LKVTableObjectUPtrWithInt64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[int64]LKVTableObjectUPtrWithInt64)
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
		shardIndex   uint32
		shard        *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
		// objKey          int64
		uObject        LKVTableObjectUPtrWithInt64
		uReleaseTarget LKVTableObjectUPtrWithInt64
	)

	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		shard = &p.Shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]

		shardRWMutex.RLock()
		for _, uObject = range *shard {
			if uObject.Ptr().GetAccessor() == 0 {
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
		p.ForceDeleteAfterReleaseDone(uReleaseTarget)
	}
}

func (p *LKVTableWithInt64) allocObjectWithInt64(objKey int64) LKVTableObjectUPtrWithInt64 {
	var uObject = LKVTableObjectUPtrWithInt64(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithInt64) TryGetObject(objKey int64) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithInt64 = 0
		shard        *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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

// MustGetObject return uObject, loaded
func (p *LKVTableWithInt64) MustGetObject(objKey int64) (LKVTableObjectUPtrWithInt64, KVTableAfterSetNewObj) {
	var (
		uObject           LKVTableObjectUPtrWithInt64 = 0
		shard             *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex      *sync.RWMutex
		isNewObjectSetted bool = false
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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
		return uObject, nil
	}

	shardRWMutex.Lock()
	uObject, _ = (*shard)[objKey]
	if uObject == 0 {
		uObject = p.allocObjectWithInt64(objKey)
		(*shard)[objKey] = uObject
		isNewObjectSetted = true
	}

	var afterSetObj KVTableAfterSetNewObj = func() {
		uObject.Ptr().Acquire()
		shardRWMutex.Unlock()
	}

	if isNewObjectSetted == false {
		afterSetObj()
		return uObject, nil
	}

	return uObject, afterSetObj
}

func (p *LKVTableWithInt64) doReleaseObject(objKey int64, isForceDeleteInMap bool) {
	var (
		uObject      LKVTableObjectUPtrWithInt64
		shard        *map[int64]LKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
		shard = &p.Shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.Lock()
	uObject, _ = (*shard)[objKey]
	if isForceDeleteInMap {
		delete(*shard, objKey)
	}
	if uObject != 0 && uObject.Ptr().GetAccessor() <= 0 {
		if p.beforeReleaseObjectFunc != nil {
			p.beforeReleaseObjectFunc(uintptr(uObject))
		}
		if isForceDeleteInMap == false {
			delete(*shard, objKey)
		}
		uObject.Ptr().Reset()
		p.objectPool.ReleaseRawObject(uintptr(uObject))
	}
	shardRWMutex.Unlock()
}

func (p *LKVTableWithInt64) ForceDeleteAfterReleaseDone(uObject LKVTableObjectUPtrWithInt64) {
	if uObject == 0 {
		return
	}
	uObject.Ptr().Release()
	if uObject.Ptr().Release() <= -1 {
		p.doReleaseObject(uObject.Ptr().ID, true)
	}
}

func (p *LKVTableWithInt64) ReleaseObject(uObject LKVTableObjectUPtrWithInt64) {
	if uObject == 0 {
		return
	}
	var accessor = uObject.Ptr().Release()
	if (accessor <= -1) ||
		(accessor == 0 && p.ReleaseObjectPolicyIsNeedRelease) {
		p.doReleaseObject(uObject.Ptr().ID, false)
	}
}
