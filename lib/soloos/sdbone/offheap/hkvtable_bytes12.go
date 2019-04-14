package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type HKVTableObjectUPtrWithBytes12 uintptr

func (u HKVTableObjectUPtrWithBytes12) Ptr() *HKVTableObjectWithBytes12 {
	return (*HKVTableObjectWithBytes12)(unsafe.Pointer(u))
}

type HKVTableObjectWithBytes12 struct {
	ID [12]byte
	HSharedPointer
}

// Heavy Key-Value table
type HKVTableWithBytes12 struct {
	KVTableCommon
	shards []map[[12]byte]HKVTableObjectUPtrWithBytes12
}

func (p *OffheapDriver) InitHKVTableWithBytes12(kvTable *HKVTableWithBytes12, name string,
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

func (p *HKVTableWithBytes12) Init(name string,
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

func (p *HKVTableWithBytes12) Name() string {
	return p.name
}

func (p *HKVTableWithBytes12) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.shards = make([]map[[12]byte]HKVTableObjectUPtrWithBytes12, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.shards[shardIndex] = make(map[[12]byte]HKVTableObjectUPtrWithBytes12)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectBytes12)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithBytes12) objectPoolInvokeReleaseObjectBytes12() {
	var (
		shardIndex      uint32
		shard           *map[[12]byte]HKVTableObjectUPtrWithBytes12
		shardRWMutex    *sync.RWMutex
		objKey          [12]byte
		uObject         HKVTableObjectUPtrWithBytes12
		uReleaseTargetK [12]byte
		uReleaseTarget  HKVTableObjectUPtrWithBytes12
	)

	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		shard = &p.shards[shardIndex]
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
		shard = &p.shards[shardIndex]
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

func (p *HKVTableWithBytes12) allocObjectWithBytes12WithReadAcquire(objKey [12]byte) HKVTableObjectUPtrWithBytes12 {
	var uObject = HKVTableObjectUPtrWithBytes12(p.objectPool.AllocRawObject())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *HKVTableWithBytes12) checkObject(v HKVTableObjectUPtrWithBytes12, objKey [12]byte) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithBytes12) TryGetObjectWithReadAcquire(objKey [12]byte) uintptr {
	var (
		uObject      HKVTableObjectUPtrWithBytes12 = 0
		shard        *map[[12]byte]HKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
		shard = &p.shards[shardIndex]
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

func (p *HKVTableWithBytes12) MustGetObjectWithReadAcquire(objKey [12]byte) (uintptr, bool) {
	var (
		uObject      HKVTableObjectUPtrWithBytes12 = 0
		shard        *map[[12]byte]HKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
		loaded       bool = false
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
		shard = &p.shards[shardIndex]
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
		uNewObject        = p.allocObjectWithBytes12WithReadAcquire(objKey)
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

func (p *HKVTableWithBytes12) DeleteObject(objKey [12]byte) {
	var (
		uObject      HKVTableObjectUPtrWithBytes12
		shard        *map[[12]byte]HKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
		shard = &p.shards[shardIndex]
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

func (p *HKVTableWithBytes12) ReadReleaseObject(uObject HKVTableObjectUPtrWithBytes12) {
	uObject.Ptr().ReadRelease()
}
