package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type HKVTableObjectUPtrWithInt64 uintptr

func (u HKVTableObjectUPtrWithInt64) Ptr() *HKVTableObjectWithInt64 {
	return (*HKVTableObjectWithInt64)(unsafe.Pointer(u))
}

type HKVTableObjectWithInt64 struct {
	ID int64
	HSharedPointer
}

// Heavy Key-Value table
type HKVTableWithInt64 struct {
	KVTableCommon
	Shards []map[int64]HKVTableObjectUPtrWithInt64
}

func (p *OffheapDriver) InitHKVTableWithInt64(kvTable *HKVTableWithInt64, name string,
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

func (p *HKVTableWithInt64) Init(name string,
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

func (p *HKVTableWithInt64) Name() string {
	return p.name
}

func (p *HKVTableWithInt64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[int64]HKVTableObjectUPtrWithInt64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[int64]HKVTableObjectUPtrWithInt64)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectInt64)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithInt64) objectPoolInvokeReleaseObjectInt64() {
	var (
		shardIndex      uint32
		shard           *map[int64]HKVTableObjectUPtrWithInt64
		shardRWMutex    *sync.RWMutex
		objKey          int64
		uObject         HKVTableObjectUPtrWithInt64
		uReleaseTargetK int64
		uReleaseTarget  HKVTableObjectUPtrWithInt64
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

func (p *HKVTableWithInt64) allocObjectWithInt64WithReadAcquire(objKey int64) HKVTableObjectUPtrWithInt64 {
	var uObject = HKVTableObjectUPtrWithInt64(p.objectPool.AllocRawObject())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *HKVTableWithInt64) checkObject(v HKVTableObjectUPtrWithInt64, objKey int64) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithInt64) TryGetObjectWithReadAcquire(objKey int64) uintptr {
	var (
		uObject      HKVTableObjectUPtrWithInt64 = 0
		shard        *map[int64]HKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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

func (p *HKVTableWithInt64) MustGetObjectWithReadAcquire(objKey int64) (HKVTableObjectUPtrWithInt64, bool) {
	var (
		uObject      HKVTableObjectUPtrWithInt64 = 0
		shard        *map[int64]HKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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
		uNewObject        = p.allocObjectWithInt64WithReadAcquire(objKey)
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

func (p *HKVTableWithInt64) DeleteObject(objKey int64) {
	var (
		uObject      HKVTableObjectUPtrWithInt64
		shard        *map[int64]HKVTableObjectUPtrWithInt64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithInt64(objKey)
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

func (p *HKVTableWithInt64) ReadReleaseObject(uObject HKVTableObjectUPtrWithInt64) {
	uObject.Ptr().ReadRelease()
}
