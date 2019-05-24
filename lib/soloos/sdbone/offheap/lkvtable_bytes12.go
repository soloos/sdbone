package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithBytes12 uintptr

func (u LKVTableObjectUPtrWithBytes12) Ptr() *LKVTableObjectWithBytes12 {
	return (*LKVTableObjectWithBytes12)(unsafe.Pointer(u))
}

type LKVTableObjectWithBytes12 struct {
	ID [12]byte
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithBytes12 struct {
	KVTableCommon
	Shards []map[[12]byte]LKVTableObjectUPtrWithBytes12

	ReleaseObjectPolicyIsNeedRelease bool
}

func (p *OffheapDriver) InitLKVTableWithBytes12(kvTable *LKVTableWithBytes12, name string,
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

func (p *LKVTableWithBytes12) Init(name string,
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

func (p *LKVTableWithBytes12) Name() string {
	return p.name
}

func (p *LKVTableWithBytes12) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[[12]byte]LKVTableObjectUPtrWithBytes12, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[[12]byte]LKVTableObjectUPtrWithBytes12)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectBytes12)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithBytes12) objectPoolInvokeReleaseObjectBytes12() {
	var (
		shardIndex   uint32
		shard        *map[[12]byte]LKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
		// objKey          [12]byte
		uObject        LKVTableObjectUPtrWithBytes12
		uReleaseTarget LKVTableObjectUPtrWithBytes12
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

func (p *LKVTableWithBytes12) allocObjectWithBytes12(objKey [12]byte) LKVTableObjectUPtrWithBytes12 {
	var uObject = LKVTableObjectUPtrWithBytes12(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithBytes12) TryGetObject(objKey [12]byte) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithBytes12 = 0
		shard        *map[[12]byte]LKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
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
func (p *LKVTableWithBytes12) MustGetObject(objKey [12]byte) (LKVTableObjectUPtrWithBytes12, KVTableAfterSetNewObj) {
	var (
		uObject           LKVTableObjectUPtrWithBytes12 = 0
		shard             *map[[12]byte]LKVTableObjectUPtrWithBytes12
		shardRWMutex      *sync.RWMutex
		isNewObjectSetted bool = false
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
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
		uObject = p.allocObjectWithBytes12(objKey)
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

func (p *LKVTableWithBytes12) doReleaseObject(objKey [12]byte, isForceDeleteInMap bool) {
	var (
		uObject      LKVTableObjectUPtrWithBytes12
		shard        *map[[12]byte]LKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
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

func (p *LKVTableWithBytes12) ForceDeleteAfterReleaseDone(uObject LKVTableObjectUPtrWithBytes12) {
	if uObject == 0 {
		return
	}
	uObject.Ptr().Release()
	if uObject.Ptr().Release() <= -1 {
		p.doReleaseObject(uObject.Ptr().ID, true)
	}
}

func (p *LKVTableWithBytes12) ReleaseObject(uObject LKVTableObjectUPtrWithBytes12) {
	if uObject == 0 {
		return
	}
	var accessor = uObject.Ptr().Release()
	if (accessor <= -1) ||
		(accessor == 0 && p.ReleaseObjectPolicyIsNeedRelease) {
		p.doReleaseObject(uObject.Ptr().ID, false)
	}
}
