package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithBytes64 uintptr

type LKVTableListObjectWithBytes64 func(obj LKVTableObjectUPtrWithBytes64) bool

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
	Shards []map[[64]byte]LKVTableObjectUPtrWithBytes64

	ReleaseObjectPolicyIsNeedRelease bool
}

func (p *OffheapDriver) InitLKVTableWithBytes64(kvTable *LKVTableWithBytes64, name string,
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

func (p *LKVTableWithBytes64) Init(name string,
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

func (p *LKVTableWithBytes64) Name() string {
	return p.name
}

func (p *LKVTableWithBytes64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[[64]byte]LKVTableObjectUPtrWithBytes64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[[64]byte]LKVTableObjectUPtrWithBytes64)
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
		shardIndex   uint32
		shard        *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
		// objKey          [64]byte
		uObject        LKVTableObjectUPtrWithBytes64
		uReleaseTarget LKVTableObjectUPtrWithBytes64
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

func (p *LKVTableWithBytes64) allocObjectWithBytes64(objKey [64]byte) LKVTableObjectUPtrWithBytes64 {
	var uObject = LKVTableObjectUPtrWithBytes64(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithBytes64) ListObject(listObject LKVTableListObjectWithBytes64) {
	var (
		uObject        LKVTableObjectUPtrWithBytes64 = 0
		shard          *map[[64]byte]LKVTableObjectUPtrWithBytes64
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

func (p *LKVTableWithBytes64) TryGetObject(objKey [64]byte) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithBytes64 = 0
		shard        *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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
func (p *LKVTableWithBytes64) MustGetObject(objKey [64]byte) (LKVTableObjectUPtrWithBytes64, KVTableAfterSetNewObj) {
	var (
		uObject           LKVTableObjectUPtrWithBytes64 = 0
		shard             *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex      *sync.RWMutex
		isNewObjectSetted bool = false
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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
		uObject = p.allocObjectWithBytes64(objKey)
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

func (p *LKVTableWithBytes64) doReleaseObject(objKey [64]byte, isForceDeleteInMap bool) {
	var (
		uObject      LKVTableObjectUPtrWithBytes64
		shard        *map[[64]byte]LKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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

func (p *LKVTableWithBytes64) ForceDeleteAfterReleaseDone(uObject LKVTableObjectUPtrWithBytes64) {
	if uObject == 0 {
		return
	}
	uObject.Ptr().Release()
	if uObject.Ptr().Release() <= -1 {
		p.doReleaseObject(uObject.Ptr().ID, true)
	}
}

func (p *LKVTableWithBytes64) ReleaseObject(uObject LKVTableObjectUPtrWithBytes64) {
	if uObject == 0 {
		return
	}
	var accessor = uObject.Ptr().Release()
	if (accessor <= -1) ||
		(accessor == 0 && p.ReleaseObjectPolicyIsNeedRelease) {
		p.doReleaseObject(uObject.Ptr().ID, false)
	}
}
