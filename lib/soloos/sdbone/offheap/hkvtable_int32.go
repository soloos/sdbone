package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type HKVTableObjectUPtrWithInt32 uintptr

func (u HKVTableObjectUPtrWithInt32) Ptr() *HKVTableObjectWithInt32 {
	return (*HKVTableObjectWithInt32)(unsafe.Pointer(u))
}

type HKVTableObjectWithInt32 struct {
	ID int32
	HSharedPointer
}

// Heavy Key-Value table
type HKVTableWithInt32 struct {
	HKVTableCommon
	shareds []map[int32]HKVTableObjectUPtrWithInt32
}

func (p *OffheapDriver) CreateHKVTableWithInt32(name string,
	objectSize int, objectsLimit int32, sharedCount uint32,
	prepareNewObjectFunc HKVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject,
) (*HKVTableWithInt32, error) {
	var (
		kvTable = new(HKVTableWithInt32)
		err     error
	)
	err = kvTable.Init(name, objectSize, objectsLimit, sharedCount,
		prepareNewObjectFunc,
		beforeReleaseObjectFunc,
	)
	if err != nil {
		return nil, err
	}

	return kvTable, err
}

func (p *HKVTableWithInt32) Init(name string,
	objectSize int, objectsLimit int32, sharedCount uint32,
	prepareNewObjectFunc HKVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject,
) error {
	var err error

	p.name = name
	p.objectSize = objectSize
	p.objectsLimit = objectsLimit

	p.sharedCount = sharedCount
	p.sharedRWMutexs = make([]sync.RWMutex, p.sharedCount)

	err = p.prepareShareds(p.objectSize, p.objectsLimit)
	if err != nil {
		return err
	}

	p.prepareNewObjectFunc = prepareNewObjectFunc
	p.beforeReleaseObjectFunc = beforeReleaseObjectFunc

	return nil
}

func (p *HKVTableWithInt32) Name() string {
	return p.name
}

func (p *HKVTableWithInt32) prepareShareds(objectSize int, objectsLimit int32) error {
	var (
		sharedIndex uint32
		err         error
	)
	p.shareds = make([]map[int32]HKVTableObjectUPtrWithInt32, p.sharedCount)
	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		p.shareds[sharedIndex] = make(map[int32]HKVTableObjectUPtrWithInt32)
	}

	err = p.chunkPool.Init(-1, objectSize, objectsLimit,
		p.chunkPoolInvokePrepareNewChunk,
		p.chunkPoolInvokeReleaseChunkInt32)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithInt32) chunkPoolInvokePrepareNewChunk(uChunk uintptr) {
	if p.prepareNewObjectFunc != nil {
		p.prepareNewObjectFunc(uChunk)
	}
}

func (p *HKVTableWithInt32) chunkPoolInvokeReleaseChunkInt32() {
	var (
		sharedIndex     uint32
		shared          *map[int32]HKVTableObjectUPtrWithInt32
		sharedRWMutex   *sync.RWMutex
		objKey          int32
		uObject         HKVTableObjectUPtrWithInt32
		uReleaseTargetK int32
		uReleaseTarget  HKVTableObjectUPtrWithInt32
	)

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.shareds[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for objKey, uObject = range *shared {
			if uObject.Ptr().IsInited() && uObject.Ptr().GetAccessor() == 0 {
				uReleaseTargetK = objKey
				uReleaseTarget = uObject
				break
			}
		}
		sharedRWMutex.RUnlock()
		if uReleaseTarget != 0 {
			goto FIND_TARGET_DONE
		}
	}

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.shareds[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for objKey, uObject = range *shared {
			if uObject.Ptr().IsInited() {
				uReleaseTargetK = objKey
				uReleaseTarget = uObject
				break
			}
		}
		sharedRWMutex.RUnlock()
		if uReleaseTarget != 0 {
			goto FIND_TARGET_DONE
		}
	}

FIND_TARGET_DONE:
	if uReleaseTarget != 0 {
		p.DeleteObject(uReleaseTargetK)
	}
}

func (p *HKVTableWithInt32) allocObjectWithInt32WithReadAcquire(objKey int32) HKVTableObjectUPtrWithInt32 {
	var uObject = HKVTableObjectUPtrWithInt32(p.chunkPool.AllocRawChunk())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTableWithInt32) checkObject(v HKVTableObjectUPtrWithInt32, objKey int32) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithInt32) TryGetObjectWithReadAcquire(objKey int32) uintptr {
	var (
		uObject       HKVTableObjectUPtrWithInt32 = 0
		shared        *map[int32]HKVTableObjectUPtrWithInt32
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithInt32(objKey)
		shared = &p.shareds[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, _ = (*shared)[objKey]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().ReadAcquire()
		if p.checkObject(uObject, objKey) == false {
			uObject.Ptr().ReadRelease()
			uObject = 0
		}
	}

	return uintptr(uObject)
}

func (p *HKVTableWithInt32) MustGetObjectWithReadAcquire(objKey int32) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrWithInt32 = 0
		shared        *map[int32]HKVTableObjectUPtrWithInt32
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedWithInt32(objKey)
		shared = &p.shareds[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, loaded = (*shared)[objKey]
	sharedRWMutex.RUnlock()

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
		uNewObject        = p.allocObjectWithInt32WithReadAcquire(objKey)
		isNewObjectSetted = false
	)

	for isNewObjectSetted == false && loaded == false {
		sharedRWMutex.Lock()
		uObject, loaded = (*shared)[objKey]
		if uObject == 0 {
			uObject = uNewObject
			(*shared)[objKey] = uObject
			isNewObjectSetted = true
		}
		sharedRWMutex.Unlock()

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
		p.chunkPool.ReleaseRawChunk(uintptr(uNewObject))
	}

	return uintptr(uObject), loaded
}

func (p *HKVTableWithInt32) DeleteObject(objKey int32) {
	var (
		uObject       HKVTableObjectUPtrWithInt32
		shared        *map[int32]HKVTableObjectUPtrWithInt32
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithInt32(objKey)
		shared = &p.shareds[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	for uObject == 0 {
		sharedRWMutex.RLock()
		uObject, _ = (*shared)[objKey]
		sharedRWMutex.RUnlock()

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
		uObject.Ptr().SetReleasable()
	}

	if uObject.Ptr().EnsureRelease() {
		sharedRWMutex.Lock()
		delete(*shared, objKey)
		sharedRWMutex.Unlock()
		uObject.Ptr().Reset()
		uObject.Ptr().WriteRelease()
		p.chunkPool.ReleaseRawChunk(uintptr(uObject))
	} else {
		uObject.Ptr().WriteRelease()
	}
}
