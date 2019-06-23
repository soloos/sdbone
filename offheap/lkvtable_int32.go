package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithInt32 uintptr

type LKVTableListObjectWithInt32 func(obj LKVTableObjectUPtrWithInt32) bool

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
		shardIndex   uint32
		shard        *map[int32]LKVTableObjectUPtrWithInt32
		shardRWMutex *sync.RWMutex
		// objKey          int32
		uObject        LKVTableObjectUPtrWithInt32
		uReleaseTarget LKVTableObjectUPtrWithInt32
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

func (p *LKVTableWithInt32) allocObjectWithInt32(objKey int32) LKVTableObjectUPtrWithInt32 {
	var uObject = LKVTableObjectUPtrWithInt32(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithInt32) ListObject(listObject LKVTableListObjectWithInt32) {
	var (
		uObject        LKVTableObjectUPtrWithInt32 = 0
		shard          *map[int32]LKVTableObjectUPtrWithInt32
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

func (p *LKVTableWithInt32) TryGetObject(objKey int32) uintptr {
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

// MustGetObject return uObject, loaded
func (p *LKVTableWithInt32) MustGetObject(objKey int32) (LKVTableObjectUPtrWithInt32, KVTableAfterSetNewObj) {
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
		return uObject, nil
	}

	shardRWMutex.Lock()
	uObject, _ = (*shard)[objKey]
	if uObject == 0 {
		uObject = p.allocObjectWithInt32(objKey)
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

func (p *LKVTableWithInt32) doReleaseObject(objKey int32, isForceDeleteInMap bool) {
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

func (p *LKVTableWithInt32) ForceDeleteAfterReleaseDone(uObject LKVTableObjectUPtrWithInt32) {
	if uObject == 0 {
		return
	}
	uObject.Ptr().Release()
	if uObject.Ptr().Release() <= -1 {
		p.doReleaseObject(uObject.Ptr().ID, true)
	}
}

func (p *LKVTableWithInt32) ReleaseObject(uObject LKVTableObjectUPtrWithInt32) {
	if uObject == 0 {
		return
	}
	var accessor = uObject.Ptr().Release()
	if (accessor <= -1) ||
		(accessor == 0 && p.ReleaseObjectPolicyIsNeedRelease) {
		p.doReleaseObject(uObject.Ptr().ID, false)
	}
}
