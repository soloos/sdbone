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
	shards []map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
}

func (p *OffheapDriver) InitLKVTableWithMagicKeyName(kvTable *LKVTableWithMagicKeyName, name string,
	objectSize int, objectsLimit int32, shardCount uint32,
	prepareNewObjectFunc KVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc KVTableInvokeBeforeReleaseObject,
) error {
	var (
		err error
	)
	err = kvTable.Init(name, objectSize, objectsLimit, shardCount,
		prepareNewObjectFunc,
		beforeReleaseObjectFunc,
	)
	if err != nil {
		return err
	}

	return err
}

func (p *LKVTableWithMagicKeyName) Init(name string,
	objectSize int, objectsLimit int32, shardCount uint32,
	prepareNewObjectFunc KVTableInvokePrepareNewObject,
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

	p.prepareNewObjectFunc = prepareNewObjectFunc
	p.beforeReleaseObjectFunc = beforeReleaseObjectFunc

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
	p.shards = make([]map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.shards[shardIndex] = make(map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName)
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
		shardIndex      uint32
		shard           *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex    *sync.RWMutex
		objKey          MagicKeyType
		uObject         LKVTableObjectUPtrWithMagicKeyName
		uReleaseTargetK MagicKeyType
		uReleaseTarget  LKVTableObjectUPtrWithMagicKeyName
	)

	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		shard = &p.shards[shardIndex]
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

func (p *LKVTableWithMagicKeyName) allocObjectWithMagicKeyNameWithAcquire(objKey MagicKeyType) LKVTableObjectUPtrWithMagicKeyName {
	var uObject = LKVTableObjectUPtrWithMagicKeyName(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithMagicKeyName) TryGetObjectWithAcquire(objKey MagicKeyType) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithMagicKeyName = 0
		shard        *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
		shard = &p.shards[shardIndex]
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

func (p *LKVTableWithMagicKeyName) MustGetObjectWithAcquire(objKey MagicKeyType) (uintptr, bool) {
	var (
		uObject      LKVTableObjectUPtrWithMagicKeyName = 0
		shard        *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
		shard = &p.shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.RLock()
	uObject, loaded = (*shard)[objKey]
	if uObject != 0 {
		uObject.Ptr().Acquire()
	}
	shardRWMutex.RUnlock()

	if uObject != 0 {
		return uintptr(uObject), loaded
	}

	shardRWMutex.Lock()
	uObject, loaded = (*shard)[objKey]
	if uObject == 0 {
		uObject = p.allocObjectWithMagicKeyNameWithAcquire(objKey)
		(*shard)[objKey] = uObject
		if p.prepareNewObjectFunc != nil {
			p.prepareNewObjectFunc(uintptr(uObject))
		}
	}
	uObject.Ptr().Acquire()
	shardRWMutex.Unlock()

	return uintptr(uObject), loaded
}

func (p *LKVTableWithMagicKeyName) DeleteObject(objKey MagicKeyType) {
	var (
		uObject      LKVTableObjectUPtrWithMagicKeyName
		shard        *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
		shard = &p.shards[shardIndex]
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

func (p *LKVTableWithMagicKeyName) ReleaseObject(uObject LKVTableObjectUPtrWithMagicKeyName) {
	var isShouldRelease = false
	if uObject.Ptr().Release() == 0 {
		isShouldRelease = true
	}

	if isShouldRelease == false {
		return
	}

	var (
		shard        *map[MagicKeyType]LKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
		objKey       = uObject.Ptr().ID
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
		shard = &p.shards[shardIndex]
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
