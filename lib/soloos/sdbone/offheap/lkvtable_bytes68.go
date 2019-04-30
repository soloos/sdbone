package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithBytes68 uintptr

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
		shardIndex      uint32
		shard           *map[[68]byte]LKVTableObjectUPtrWithBytes68
		shardRWMutex    *sync.RWMutex
		objKey          [68]byte
		uObject         LKVTableObjectUPtrWithBytes68
		uReleaseTargetK [68]byte
		uReleaseTarget  LKVTableObjectUPtrWithBytes68
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

func (p *LKVTableWithBytes68) allocObjectWithBytes68WithAcquire(objKey [68]byte) LKVTableObjectUPtrWithBytes68 {
	var uObject = LKVTableObjectUPtrWithBytes68(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithBytes68) TryGetObjectWithAcquire(objKey [68]byte) uintptr {
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

// MustGetObjectWithAcquire return uObject, loaded
func (p *LKVTableWithBytes68) MustGetObjectWithAcquire(objKey [68]byte) (uintptr, KVTableAfterSetNewObj) {
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
		return uintptr(uObject), nil
	}

	shardRWMutex.Lock()
	uObject, _ = (*shard)[objKey]
	var afterSetObj KVTableAfterSetNewObj = func() {
		uObject.Ptr().Acquire()
		shardRWMutex.Unlock()
	}
	if uObject == 0 {
		uObject = p.allocObjectWithBytes68WithAcquire(objKey)
		(*shard)[objKey] = uObject
		isNewObjectSetted = true
	}

	if isNewObjectSetted == false {
		afterSetObj()
		return uintptr(uObject), nil
	}

	return uintptr(uObject), afterSetObj
}

func (p *LKVTableWithBytes68) DeleteObject(objKey [68]byte) {
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
	if uObject != 0 && uObject.Ptr().GetAccessor() == 0 {
		if p.beforeReleaseObjectFunc != nil {
			p.beforeReleaseObjectFunc(uintptr(uObject))
		}
		delete(*shard, objKey)
		p.objectPool.ReleaseRawObject(uintptr(uObject))
	}
	shardRWMutex.Lock()
}

func (p *LKVTableWithBytes68) ReleaseObject(uObject LKVTableObjectUPtrWithBytes68) {
	var isShouldRelease = (uObject.Ptr().Release() == 0) && p.ReleaseObjectPolicyIsNeedRelease
	if isShouldRelease == false {
		return
	}

	var (
		shard        *map[[68]byte]LKVTableObjectUPtrWithBytes68
		shardRWMutex *sync.RWMutex
		objKey       = uObject.Ptr().ID
	)

	{
		shardIndex := p.GetShardWithBytes68(objKey)
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
