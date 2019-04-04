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
	HKVTableCommon
	shareds []map[[64]byte]HKVTableObjectUPtrWithBytes64
}

func (p *OffheapDriver) CreateHKVTableWithBytes64(name string,
	objectSize int, objectsLimit int32, sharedCount uint32,
	prepareNewObjectFunc HKVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject,
) (*HKVTableWithBytes64, error) {
	var (
		kvTable = new(HKVTableWithBytes64)
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

func (p *HKVTableWithBytes64) Init(name string,
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

func (p *HKVTableWithBytes64) Name() string {
	return p.name
}

func (p *HKVTableWithBytes64) prepareShareds(objectSize int, objectsLimit int32) error {
	var (
		sharedIndex uint32
		err         error
	)
	p.shareds = make([]map[[64]byte]HKVTableObjectUPtrWithBytes64, p.sharedCount)
	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		p.shareds[sharedIndex] = make(map[[64]byte]HKVTableObjectUPtrWithBytes64)
	}

	err = p.chunkPool.Init(-1, objectSize, objectsLimit,
		p.chunkPoolInvokePrepareNewChunk,
		p.chunkPoolInvokeReleaseChunkBytes64)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithBytes64) chunkPoolInvokePrepareNewChunk(uChunk uintptr) {
	if p.prepareNewObjectFunc != nil {
		p.prepareNewObjectFunc(uChunk)
	}
}

func (p *HKVTableWithBytes64) chunkPoolInvokeReleaseChunkBytes64() {
	var (
		sharedIndex     uint32
		shared          *map[[64]byte]HKVTableObjectUPtrWithBytes64
		sharedRWMutex   *sync.RWMutex
		objKey          [64]byte
		uObject         HKVTableObjectUPtrWithBytes64
		uReleaseTargetK [64]byte
		uReleaseTarget  HKVTableObjectUPtrWithBytes64
	)

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.shareds[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for objKey, uObject = range *shared {
			if uObject.Ptr().GetAccessor() == 0 {
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

	if uReleaseTarget != 0 {
		goto FIND_TARGET_DONE
	}

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.shareds[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for objKey, uObject = range *shared {
			uReleaseTargetK = objKey
			uReleaseTarget = uObject
			break
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

func (p *HKVTableWithBytes64) allocObjectWithBytes64WithReadAcquire(objKey [64]byte) HKVTableObjectUPtrWithBytes64 {
	var uObject = HKVTableObjectUPtrWithBytes64(p.chunkPool.AllocRawChunk())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTableWithBytes64) checkObject(v HKVTableObjectUPtrWithBytes64, objKey [64]byte) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithBytes64) MustGetObjectWithReadAcquire(objKey [64]byte) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrWithBytes64 = 0
		shared        *map[[64]byte]HKVTableObjectUPtrWithBytes64
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedWithBytes64(objKey)
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
		uNewObject        HKVTableObjectUPtrWithBytes64 = p.allocObjectWithBytes64WithReadAcquire(objKey)
		isNewObjectSetted bool                          = false
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

func (p *HKVTableWithBytes64) TryGetObjectWithReadAcquire(objKey [64]byte) uintptr {
	var (
		uObject       HKVTableObjectUPtrWithBytes64 = 0
		shared        *map[[64]byte]HKVTableObjectUPtrWithBytes64
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithBytes64(objKey)
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

func (p *HKVTableWithBytes64) DeleteObject(objKey [64]byte) {
	var (
		uObject       HKVTableObjectUPtrWithBytes64
		shared        *map[[64]byte]HKVTableObjectUPtrWithBytes64
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithBytes64(objKey)
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
			continue
		}
	}

	// assert uObject != 0

	for {
		if p.beforeReleaseObjectFunc != nil {
			p.beforeReleaseObjectFunc(uintptr(uObject))
		} else {
			uObject.Ptr().SetReleasable()
		}

		if uObject.Ptr().IsShouldRelease() {
			sharedRWMutex.Lock()
			delete(*shared, objKey)
			sharedRWMutex.Unlock()
			uObject.Ptr().Reset()
			uObject.Ptr().WriteRelease()
			p.chunkPool.ReleaseRawChunk(uintptr(uObject))
			break
		}
	}
}
