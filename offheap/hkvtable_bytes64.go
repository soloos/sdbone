package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type HKVTableObjectUPtrWithBytes64 uintptr

func (u HKVTableObjectUPtrWithBytes64) Ptr() *HKVTableObjectWithBytes64 {
	return (*HKVTableObjectWithBytes64)(unsafe.Pointer(u))
}

type HKVTableObjectWithBytes64 struct {
	ID [64]byte
	HSharedPointer
}

// Heavy Key-Value table
type HKVTableWithBytes64 struct {
	KVTableCommon
	Shards []map[[64]byte]HKVTableObjectUPtrWithBytes64
}

func (p *OffheapDriver) InitHKVTableWithBytes64(kvTable *HKVTableWithBytes64, name string,
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

func (p *HKVTableWithBytes64) Init(name string,
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

func (p *HKVTableWithBytes64) Name() string {
	return p.name
}

func (p *HKVTableWithBytes64) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.Shards = make([]map[[64]byte]HKVTableObjectUPtrWithBytes64, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.Shards[shardIndex] = make(map[[64]byte]HKVTableObjectUPtrWithBytes64)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectBytes64)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithBytes64) objectPoolInvokeReleaseObjectBytes64() {
	var (
		shardIndex      uint32
		shard           *map[[64]byte]HKVTableObjectUPtrWithBytes64
		shardRWMutex    *sync.RWMutex
		objKey          [64]byte
		uObject         HKVTableObjectUPtrWithBytes64
		uReleaseTargetK [64]byte
		uReleaseTarget  HKVTableObjectUPtrWithBytes64
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

func (p *HKVTableWithBytes64) allocObjectWithBytes64WithReadAcquire(objKey [64]byte) HKVTableObjectUPtrWithBytes64 {
	var uObject = HKVTableObjectUPtrWithBytes64(p.objectPool.AllocRawObject())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *HKVTableWithBytes64) checkObject(v HKVTableObjectUPtrWithBytes64, objKey [64]byte) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithBytes64) TryGetObjectWithReadAcquire(objKey [64]byte) uintptr {
	var (
		uObject      HKVTableObjectUPtrWithBytes64 = 0
		shard        *map[[64]byte]HKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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

func (p *HKVTableWithBytes64) MustGetObjectWithReadAcquire(objKey [64]byte) (HKVTableObjectUPtrWithBytes64, bool) {
	var (
		uObject      HKVTableObjectUPtrWithBytes64 = 0
		shard        *map[[64]byte]HKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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
		uNewObject        = p.allocObjectWithBytes64WithReadAcquire(objKey)
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

func (p *HKVTableWithBytes64) DeleteObject(objKey [64]byte) {
	var (
		uObject      HKVTableObjectUPtrWithBytes64
		shard        *map[[64]byte]HKVTableObjectUPtrWithBytes64
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes64(objKey)
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

func (p *HKVTableWithBytes64) ReadReleaseObject(uObject HKVTableObjectUPtrWithBytes64) {
	uObject.Ptr().ReadRelease()
}
