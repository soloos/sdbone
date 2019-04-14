package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type LKVTableObjectUPtrWithBytes12 uintptr

func (u LKVTableObjectUPtrWithBytes12) Ptr() *LKVTableObjectWithBytes12 {
	return (*LKVTableObjectWithBytes12)(unsafe.Pointer(u))
}

type LKVTableObjectWithBytes12 struct {
	ID [12]byte
	LSharedPointer
}

// Heavy Key-Value table
type LKVTableWithBytes12 struct {
	KVTableCommon
	shards []map[[12]byte]LKVTableObjectUPtrWithBytes12
}

func (p *OffheapDriver) InitLKVTableWithBytes12(kvTable *LKVTableWithBytes12, name string,
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

func (p *LKVTableWithBytes12) Init(name string,
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

func (p *LKVTableWithBytes12) Name() string {
	return p.name
}

func (p *LKVTableWithBytes12) prepareShards(objectSize int, objectsLimit int32) error {
	var (
		shardIndex uint32
		err        error
	)
	p.shards = make([]map[[12]byte]LKVTableObjectUPtrWithBytes12, p.shardCount)
	for shardIndex = 0; shardIndex < p.shardCount; shardIndex++ {
		p.shards[shardIndex] = make(map[[12]byte]LKVTableObjectUPtrWithBytes12)
	}

	err = p.objectPool.Init(objectSize, objectsLimit,
		nil, p.objectPoolInvokeReleaseObjectBytes12)
	if err != nil {
		return err
	}

	return nil
}

func (p *LKVTableWithBytes12) objectPoolInvokeReleaseObjectBytes12() {
	var (
		shardIndex      uint32
		shard           *map[[12]byte]LKVTableObjectUPtrWithBytes12
		shardRWMutex    *sync.RWMutex
		objKey          [12]byte
		uObject         LKVTableObjectUPtrWithBytes12
		uReleaseTargetK [12]byte
		uReleaseTarget  LKVTableObjectUPtrWithBytes12
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

func (p *LKVTableWithBytes12) allocObjectWithBytes12WithAcquire(objKey [12]byte) LKVTableObjectUPtrWithBytes12 {
	var uObject = LKVTableObjectUPtrWithBytes12(p.objectPool.AllocRawObject())
	uObject.Ptr().Acquire()
	uObject.Ptr().ID = objKey
	return uObject
}

func (p *LKVTableWithBytes12) TryGetObjectWithAcquire(objKey [12]byte) uintptr {
	var (
		uObject      LKVTableObjectUPtrWithBytes12 = 0
		shard        *map[[12]byte]LKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
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

func (p *LKVTableWithBytes12) MustGetObjectWithAcquire(objKey [12]byte) (uintptr, bool) {
	var (
		uObject      LKVTableObjectUPtrWithBytes12 = 0
		shard        *map[[12]byte]LKVTableObjectUPtrWithBytes12
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
		uObject = p.allocObjectWithBytes12WithAcquire(objKey)
		(*shard)[objKey] = uObject
		if p.prepareNewObjectFunc != nil {
			p.prepareNewObjectFunc(uintptr(uObject))
		}
	}
	uObject.Ptr().Acquire()
	shardRWMutex.Unlock()

	return uintptr(uObject), loaded
}

func (p *LKVTableWithBytes12) DeleteObject(objKey [12]byte) {
	var (
		uObject      LKVTableObjectUPtrWithBytes12
		shard        *map[[12]byte]LKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
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

func (p *LKVTableWithBytes12) ReleaseObject(uObject LKVTableObjectUPtrWithBytes12) {
	var isShouldRelease = false
	if uObject.Ptr().Release() == 0 {
		isShouldRelease = true
	}

	if isShouldRelease == false {
		return
	}

	var (
		shard        *map[[12]byte]LKVTableObjectUPtrWithBytes12
		shardRWMutex *sync.RWMutex
		objKey       = uObject.Ptr().ID
	)

	{
		shardIndex := p.GetShardWithBytes12(objKey)
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