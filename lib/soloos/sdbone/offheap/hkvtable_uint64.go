package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type HKVTableObjectUPtrWithUint64 uintptr

func (u HKVTableObjectUPtrWithUint64) Ptr() *HKVTableObjectWithUint64 {
	return (*HKVTableObjectWithUint64)(unsafe.Pointer(u))
}

type HKVTableObjectWithUint64 struct {
	ID uint64
	HSharedPointer
}

// Heavy Key-Value table
type HKVTableWithUint64 struct {
	KVTableCommon
	Shards []map[uint64]HKVTableObjectUPtrWithUint64
}

func (p *OffheapDriver) InitHKVTableWithUint64(kvTable *HKVTableWithUint64, name string,
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

func (p *HKVTableWithUint64) Init(name string,
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

func (p *HKVTableWithUint64) Name() string {
	return p.name
}

func (p *HKVTableWithUint64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[uint64]HKVTableObjectUPtrWithUint64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[uint64]HKVTableObjectUPtrWithUint64)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectUint64)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithUint64) objectPoolInvokeReleaseObjectUint64() {
	var (
		shardIndex      uint32
		shard           *map[uint64]HKVTableObjectUPtrWithUint64
		shardRWMutex    *sync.RWMutex
		objKey          uint64
		uObject         HKVTableObjectUPtrWithUint64
		uReleaseTargetK uint64
		uReleaseTarget  HKVTableObjectUPtrWithUint64
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

func (p *HKVTableWithUint64) allocObjectWithUint64WithReadAcquire(objKey uint64) HKVTableObjectUPtrWithUint64 {
	var uObject = HKVTableObjectUPtrWithUint64(p.objectPool.AllocRawObject())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *HKVTableWithUint64) checkObject(v HKVTableObjectUPtrWithUint64, objKey uint64) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithUint64) TryGetObjectWithReadAcquire(objKey uint64) uintptr {
	var (
		uObject      HKVTableObjectUPtrWithUint64 = 0
		shard        *map[uint64]HKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
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

func (p *HKVTableWithUint64) MustGetObjectWithReadAcquire(objKey uint64) (uintptr, bool) {
	var (
		uObject      HKVTableObjectUPtrWithUint64 = 0
		shard        *map[uint64]HKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
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
		return uintptr(uObject), loaded
	}

	var (
		uNewObject        = p.allocObjectWithUint64WithReadAcquire(objKey)
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

	return uintptr(uObject), loaded
}

func (p *HKVTableWithUint64) DeleteObject(objKey uint64) {
	var (
		uObject      HKVTableObjectUPtrWithUint64
		shard        *map[uint64]HKVTableObjectUPtrWithUint64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithUint64(objKey)
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

func (p *HKVTableWithUint64) ReadReleaseObject(uObject HKVTableObjectUPtrWithUint64) {
	uObject.Ptr().ReadRelease()
}
