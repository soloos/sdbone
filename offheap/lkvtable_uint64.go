package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithUint64 uintptr

type LKVTableListObjectWithUint64 func(obj LKVTableObjectUPtrWithUint64) bool

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
	Shards []map[uint64]LKVTableObjectUPtrWithUint64

	ReleaseObjectPolicyIsNeedRelease bool
}

func (p *OffheapDriver) InitLKVTableWithUint64(kvTable *LKVTableWithUint64, name string,
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

func (p *LKVTableWithUint64) Init(name string,
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

func (p *LKVTableWithUint64) Name() string {
	return p.name
}

func (p *LKVTableWithUint64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[uint64]LKVTableObjectUPtrWithUint64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[uint64]LKVTableObjectUPtrWithUint64)
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
		shardIndex   uint32
		shard        *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
		// objKey          uint64
		uObject        LKVTableObjectUPtrWithUint64
		uReleaseTarget LKVTableObjectUPtrWithUint64
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

func (p *LKVTableWithUint64) allocObjectWithUint64(objKey uint64) LKVTableObjectUPtrWithUint64 {
	var uObject = LKVTableObjectUPtrWithUint64(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithUint64) ListObject(listObject LKVTableListObjectWithUint64) {
	var (
		uObject        LKVTableObjectUPtrWithUint64 = 0
		shard          *map[uint64]LKVTableObjectUPtrWithUint64
		isListContinue bool
	)

	for shardIndex, _ := range p.Shards {
		isListContinue = true
		shard = &p.Shards[shardIndex]
		p.shardRWMutexs[shardIndex].RLock()
		for _, uObject = range *shard {
			isListContinue = listObject(uObject)
			if isListContinue == false {
				break
			}
		}
		p.shardRWMutexs[shardIndex].RUnlock()

		if isListContinue == false {
			break
		}
	}
}

func (p *LKVTableWithUint64) TryGetObject(objKey uint64) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithUint64 = 0
		shard        *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
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
func (p *LKVTableWithUint64) MustGetObject(objKey uint64) (LKVTableObjectUPtrWithUint64, KVTableAfterSetNewObj) {
	var (
		uObject           LKVTableObjectUPtrWithUint64 = 0
		shard             *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex      *sync.RWMutex
		isNewObjectSetted bool = false
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
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
		uObject = p.allocObjectWithUint64(objKey)
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

func (p *LKVTableWithUint64) doReleaseObject(objKey uint64, isForceDeleteInMap bool) {
	var (
		uObject      LKVTableObjectUPtrWithUint64
		shard        *map[uint64]LKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
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

func (p *LKVTableWithUint64) ForceDeleteAfterReleaseDone(uObject LKVTableObjectUPtrWithUint64) {
	if uObject == 0 {
		return
	}
	uObject.Ptr().Release()
	if uObject.Ptr().Release() <= -1 {
		p.doReleaseObject(uObject.Ptr().ID, true)
	}
}

func (p *LKVTableWithUint64) ReleaseObject(uObject LKVTableObjectUPtrWithUint64) {
	if uObject == 0 {
		return
	}
	var accessor = uObject.Ptr().Release()
	if (accessor <= -1) ||
		(accessor == 0 && p.ReleaseObjectPolicyIsNeedRelease) {
		p.doReleaseObject(uObject.Ptr().ID, false)
	}
}
