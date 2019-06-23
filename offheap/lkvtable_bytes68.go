package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithBytes68 uintptr

type LKVTableListObjectWithBytes68 func(obj LKVTableObjectUPtrWithBytes68) bool

func (u LKVTableObjectUPtrWithBytes68) Ptr() *LKVTableObjectWithBytes68 {
	return (*LKVTableObjectWithBytes68)(unsafe.Pointer(u))
}

type LKVTableObjectWithBytes68 struct {
	ID [68]byte
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithBytes68 struct {
	KVTableCommon
	Shards []map[[68]byte]LKVTableObjectUPtrWithBytes68

	ReleaseObjectPolicyIsNeedRelease bool
}

func (p *OffheapDriver) InitLKVTableWithBytes68(kvTable *LKVTableWithBytes68, name string,
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

func (p *LKVTableWithBytes68) Init(name string,
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

func (p *LKVTableWithBytes68) Name() string {
	return p.name
}

func (p *LKVTableWithBytes68) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[[68]byte]LKVTableObjectUPtrWithBytes68, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[[68]byte]LKVTableObjectUPtrWithBytes68)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectBytes68)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithBytes68) objectPoolInvokeReleaseObjectBytes68() {
	var (
		shardIndex   uint32
		shard        *map[[68]byte]LKVTableObjectUPtrWithBytes68
		shardRWMutex *sync.RWMutex
		// objKey          [68]byte
		uObject        LKVTableObjectUPtrWithBytes68
		uReleaseTarget LKVTableObjectUPtrWithBytes68
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

func (p *LKVTableWithBytes68) allocObjectWithBytes68(objKey [68]byte) LKVTableObjectUPtrWithBytes68 {
	var uObject = LKVTableObjectUPtrWithBytes68(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithBytes68) ListObject(listObject LKVTableListObjectWithBytes68) {
	var (
		uObject        LKVTableObjectUPtrWithBytes68 = 0
		shard          *map[[68]byte]LKVTableObjectUPtrWithBytes68
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

func (p *LKVTableWithBytes68) TryGetObject(objKey [68]byte) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithBytes68 = 0
		shard        *map[[68]byte]LKVTableObjectUPtrWithBytes68
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes68(objKey)
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
func (p *LKVTableWithBytes68) MustGetObject(objKey [68]byte) (LKVTableObjectUPtrWithBytes68, KVTableAfterSetNewObj) {
	var (
		uObject           LKVTableObjectUPtrWithBytes68 = 0
		shard             *map[[68]byte]LKVTableObjectUPtrWithBytes68
		shardRWMutex      *sync.RWMutex
		isNewObjectSetted bool = false
	)

	{
		shardIndex := p.GetShardWithBytes68(objKey)
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
		uObject = p.allocObjectWithBytes68(objKey)
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

func (p *LKVTableWithBytes68) doReleaseObject(objKey [68]byte, isForceDeleteInMap bool) {
	var (
		uObject      LKVTableObjectUPtrWithBytes68
		shard        *map[[68]byte]LKVTableObjectUPtrWithBytes68
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes68(objKey)
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

func (p *LKVTableWithBytes68) ForceDeleteAfterReleaseDone(uObject LKVTableObjectUPtrWithBytes68) {
	if uObject == 0 {
		return
	}
	uObject.Ptr().Release()
	if uObject.Ptr().Release() <= -1 {
		p.doReleaseObject(uObject.Ptr().ID, true)
	}
}

func (p *LKVTableWithBytes68) ReleaseObject(uObject LKVTableObjectUPtrWithBytes68) {
	if uObject == 0 {
		return
	}
	var accessor = uObject.Ptr().Release()
	if (accessor <= -1) ||
		(accessor == 0 && p.ReleaseObjectPolicyIsNeedRelease) {
		p.doReleaseObject(uObject.Ptr().ID, false)
	}
}
