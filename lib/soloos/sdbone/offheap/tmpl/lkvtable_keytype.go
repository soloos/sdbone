package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithMagicKeyName uintptr

func (u LKVTableObjectUPtrWithMagicKeyName) Ptr() *LKVTableObjectWithMagicKeyName {
	return (*LKVTableObjectWithMagicKeyName)(unsafe.Pointer(u))
}

type LKVTableObjectWithMagicKeyName struct {
	ID MagicKeyType
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithMagicKeyName struct {
	KVTableCommon
	Shards []map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName

	ReleaseObjectPolicyIsNeedRelease bool
}

func (p *OffheapDriver) InitLKVTableWithMagicKeyName(kvTable *LKVTableWithMagicKeyName, name string,
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

func (p *LKVTableWithMagicKeyName) Init(name string,
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

func (p *LKVTableWithMagicKeyName) Name() string {
	return p.name
}

func (p *LKVTableWithMagicKeyName) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectMagicKeyName)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithMagicKeyName) objectPoolInvokeReleaseObjectMagicKeyName() {
	var (
		shardIndex   uint32
		shard        *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
		// objKey          MagicKeyType
		uObject        LKVTableObjectUPtrWithMagicKeyName
		uReleaseTarget LKVTableObjectUPtrWithMagicKeyName
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

func (p *LKVTableWithMagicKeyName) allocObjectWithMagicKeyName(objKey MagicKeyType) LKVTableObjectUPtrWithMagicKeyName {
	var uObject = LKVTableObjectUPtrWithMagicKeyName(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithMagicKeyName) TryGetObject(objKey MagicKeyType) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithMagicKeyName = 0
		shard        *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
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
func (p *LKVTableWithMagicKeyName) MustGetObject(objKey MagicKeyType) (LKVTableObjectUPtrWithMagicKeyName, KVTableAfterSetNewObj) {
	var (
		uObject           LKVTableObjectUPtrWithMagicKeyName = 0
		shard             *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex      *sync.RWMutex
		isNewObjectSetted bool = false
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
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
	var afterSetObj KVTableAfterSetNewObj = func() {
		uObject.Ptr().Acquire()
		shardRWMutex.Unlock()
	}
	if uObject == 0 {
		uObject = p.allocObjectWithMagicKeyName(objKey)
		(*shard)[objKey] = uObject
		isNewObjectSetted = true
	}

	if isNewObjectSetted == false {
		afterSetObj()
		return uObject, nil
	}

	return uObject, afterSetObj
}

func (p *LKVTableWithMagicKeyName) doReleaseObject(objKey MagicKeyType, isForceDeleteInMap bool) {
	var (
		uObject      LKVTableObjectUPtrWithMagicKeyName
		shard        *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
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
		p.objectPool.ReleaseRawObject(uintptr(uObject))
	}
	shardRWMutex.Unlock()
}

func (p *LKVTableWithMagicKeyName) ForceDeleteAfterReleaseDone(uObject LKVTableObjectUPtrWithMagicKeyName) {
	if uObject == 0 {
		return
	}
	uObject.Ptr().Release()
	if uObject.Ptr().Release() == -1 {
		p.doReleaseObject(uObject.Ptr().ID, true)
	}
}

func (p *LKVTableWithMagicKeyName) ReleaseObject(uObject LKVTableObjectUPtrWithMagicKeyName) {
	if uObject == 0 {
		return
	}
	var accessor = uObject.Ptr().Release()
	if (accessor == -1) ||
		(accessor == 0 && p.ReleaseObjectPolicyIsNeedRelease) {
		p.doReleaseObject(uObject.Ptr().ID, false)
	}
}
