package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithString uintptr

type LKVTableListObjectWithString func(obj LKVTableObjectUPtrWithString) bool

func (u LKVTableObjectUPtrWithString) Ptr() *LKVTableObjectWithString {
	return (*LKVTableObjectWithString)(unsafe.Pointer(u))
}

type LKVTableObjectWithString struct {
	ID string
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithString struct {
	KVTableCommon
	Shards []map[string]LKVTableObjectUPtrWithString

	ReleaseObjectPolicyIsNeedRelease bool
}

func (p *OffheapDriver) InitLKVTableWithString(kvTable *LKVTableWithString, name string,
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

func (p *LKVTableWithString) Init(name string,
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

func (p *LKVTableWithString) Name() string {
	return p.name
}

func (p *LKVTableWithString) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[string]LKVTableObjectUPtrWithString, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[string]LKVTableObjectUPtrWithString)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectString)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithString) objectPoolInvokeReleaseObjectString() {
	var (
		shardIndex   uint32
		shard        *map[string]LKVTableObjectUPtrWithString
		shardRWMutex *sync.RWMutex
		// objKey          string
		uObject        LKVTableObjectUPtrWithString
		uReleaseTarget LKVTableObjectUPtrWithString
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

func (p *LKVTableWithString) allocObjectWithString(objKey string) LKVTableObjectUPtrWithString {
	var uObject = LKVTableObjectUPtrWithString(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithString) ListObject(listObject LKVTableListObjectWithString) {
	var (
		uObject        LKVTableObjectUPtrWithString = 0
		shard          *map[string]LKVTableObjectUPtrWithString
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

func (p *LKVTableWithString) TryGetObject(objKey string) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithString = 0
		shard        *map[string]LKVTableObjectUPtrWithString
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithString(objKey)
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
func (p *LKVTableWithString) MustGetObject(objKey string) (LKVTableObjectUPtrWithString, KVTableAfterSetNewObj) {
	var (
		uObject           LKVTableObjectUPtrWithString = 0
		shard             *map[string]LKVTableObjectUPtrWithString
		shardRWMutex      *sync.RWMutex
		isNewObjectSetted bool = false
	)

	{
		shardIndex := p.GetShardWithString(objKey)
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
		uObject = p.allocObjectWithString(objKey)
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

func (p *LKVTableWithString) doReleaseObject(objKey string, isForceDeleteInMap bool) {
	var (
		uObject      LKVTableObjectUPtrWithString
		shard        *map[string]LKVTableObjectUPtrWithString
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithString(objKey)
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

func (p *LKVTableWithString) ForceDeleteAfterReleaseDone(uObject LKVTableObjectUPtrWithString) {
	if uObject == 0 {
		return
	}
	uObject.Ptr().Release()
	if uObject.Ptr().Release() <= -1 {
		p.doReleaseObject(uObject.Ptr().ID, true)
	}
}

func (p *LKVTableWithString) ReleaseObject(uObject LKVTableObjectUPtrWithString) {
	if uObject == 0 {
		return
	}
	var accessor = uObject.Ptr().Release()
	if (accessor <= -1) ||
		(accessor == 0 && p.ReleaseObjectPolicyIsNeedRelease) {
		p.doReleaseObject(uObject.Ptr().ID, false)
	}
}
