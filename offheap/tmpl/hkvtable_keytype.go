package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type HKVTableObjectUPtrWithMagicKeyName uintptr

func (u HKVTableObjectUPtrWithMagicKeyName) Ptr() *HKVTableObjectWithMagicKeyName {
	return (*HKVTableObjectWithMagicKeyName)(unsafe.Pointer(u))
}

type HKVTableObjectWithMagicKeyName struct {
	ID MagicKeyType
	HSharedPointer
}

// Heavy Key-Value table
type HKVTableWithMagicKeyName struct {
	KVTableCommon
	Shards []map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
}

func (p *OffheapDriver) InitHKVTableWithMagicKeyName(kvTable *HKVTableWithMagicKeyName, name string,
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

func (p *HKVTableWithMagicKeyName) Init(name string,
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

func (p *HKVTableWithMagicKeyName) Name() string {
	return p.name
}

func (p *HKVTableWithMagicKeyName) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectMagicKeyName)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithMagicKeyName) objectPoolInvokeReleaseObjectMagicKeyName() {
	var (
		shardIndex      uint32
		shard           *map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
		shardRWMutex    *sync.RWMutex
		objKey          MagicKeyType
		uObject         HKVTableObjectUPtrWithMagicKeyName
		uReleaseTargetK MagicKeyType
		uReleaseTarget  HKVTableObjectUPtrWithMagicKeyName
	)

	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		shard = &p.Shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]

		shardRWMutex.RLock()
		for objKey, uObject = range *shard {
			if uObject.Ptr().IsInited() && uObject.Ptr().GetAccessor() == 0 {
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

	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		shard = &p.Shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]

		shardRWMutex.RLock()
		for objKey, uObject = range *shard {
			if uObject.Ptr().IsInited() {
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

func (p *HKVTableWithMagicKeyName) allocObjectWithMagicKeyNameWithReadAcquire(objKey MagicKeyType) HKVTableObjectUPtrWithMagicKeyName {
	var uObject = HKVTableObjectUPtrWithMagicKeyName(p.objectPool.AllocRawObject())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *HKVTableWithMagicKeyName) checkObject(v HKVTableObjectUPtrWithMagicKeyName, objKey MagicKeyType) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithMagicKeyName) TryGetObjectWithReadAcquire(objKey MagicKeyType) uintptr {
	var (
		uObject      HKVTableObjectUPtrWithMagicKeyName = 0
		shard        *map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
		shard = &p.Shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.RLock()
	uObject, _ = (*shard)[objKey]
	shardRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().ReadAcquire()
		if p.checkObject(uObject, objKey) == false {
			uObject.Ptr().ReadRelease()
			uObject = 0
		}
	}

	return uintptr(uObject)
}

func (p *HKVTableWithMagicKeyName) MustGetObjectWithReadAcquire(objKey MagicKeyType) (HKVTableObjectUPtrWithMagicKeyName, bool) {
	var (
		uObject      HKVTableObjectUPtrWithMagicKeyName = 0
		shard        *map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
		shard = &p.Shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	shardRWMutex.RLock()
	uObject, loaded = (*shard)[objKey]
	shardRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().ReadAcquire()
		if p.checkObject(uObject, objKey) == false {
			uObject.Ptr().ReadRelease()
			uObject = 0
			loaded = false
		} else {
			loaded = true
		}
	}

	if uObject != 0 {
		return uObject, loaded
	}

	var (
		uNewObject        = p.allocObjectWithMagicKeyNameWithReadAcquire(objKey)
		isNewObjectSetted = false
	)

	for isNewObjectSetted == false && loaded == false {
		shardRWMutex.Lock()
		uObject, loaded = (*shard)[objKey]
		if uObject == 0 {
			uObject = uNewObject
			(*shard)[objKey] = uObject
			isNewObjectSetted = true
		}
		shardRWMutex.Unlock()

		if isNewObjectSetted == false {
			uObject.Ptr().ReadAcquire()
			if p.checkObject(uObject, objKey) == false {
				uObject.Ptr().ReadRelease()
				uObject = 0
				loaded = false
			} else {
				loaded = true
			}
		}
	}

	if isNewObjectSetted == false {
		uNewObject.Ptr().Reset()
		uNewObject.Ptr().ReadRelease()
		p.objectPool.ReleaseRawObject(uintptr(uNewObject))
	} else {
		if p.prepareNewObjectFunc != nil {
			p.prepareNewObjectFunc(uintptr(uObject))
		}
	}

	return uObject, loaded
}

func (p *HKVTableWithMagicKeyName) DeleteObject(objKey MagicKeyType) {
	var (
		uObject      HKVTableObjectUPtrWithMagicKeyName
		shard        *map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithMagicKeyName(objKey)
		shard = &p.Shards[shardIndex]
		shardRWMutex = &p.shardRWMutexs[shardIndex]
	}

	for uObject == 0 {
		shardRWMutex.RLock()
		uObject, _ = (*shard)[objKey]
		shardRWMutex.RUnlock()

		if uObject == 0 {
			return
		}

		uObject.Ptr().WriteAcquire()
		if p.checkObject(uObject, objKey) == false {
			uObject.Ptr().WriteRelease()
			uObject = 0
		}
	}

	// assert uObject != 0

	if p.beforeReleaseObjectFunc != nil {
		p.beforeReleaseObjectFunc(uintptr(uObject))
	} else {
		uObject.Ptr().Reset()
		uObject.Ptr().SetReleasable()
	}

	if uObject.Ptr().EnsureRelease() {
		shardRWMutex.Lock()
		delete(*shard, objKey)
		shardRWMutex.Unlock()
		uObject.Ptr().WriteRelease()
		p.objectPool.ReleaseRawObject(uintptr(uObject))
	} else {
		uObject.Ptr().WriteRelease()
	}
}

func (p *HKVTableWithMagicKeyName) ReadReleaseObject(uObject HKVTableObjectUPtrWithMagicKeyName) {
	uObject.Ptr().ReadRelease()
}
