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
	HKVTableCommon
	shareds []map[[12]byte]HKVTableObjectUPtrWithBytes12
}

func (p *OffheapDriver) CreateHKVTableWithBytes12(name string,
	objectSize int, objectsLimit int32, sharedCount uint32,
	prepareNewObjectFunc HKVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject,
) (*HKVTableWithBytes12, error) {
	var (
		kvTable = new(HKVTableWithBytes12)
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

func (p *HKVTableWithBytes12) Init(name string,
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

func (p *HKVTableWithBytes12) Name() string {
	return p.name
}

func (p *HKVTableWithBytes12) prepareShareds(objectSize int, objectsLimit int32) error {
	var (
		sharedIndex uint32
		err         error
	)
	p.shareds = make([]map[[12]byte]HKVTableObjectUPtrWithBytes12, p.sharedCount)
	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		p.shareds[sharedIndex] = make(map[[12]byte]HKVTableObjectUPtrWithBytes12)
	}

	err = p.chunkPool.Init(-1, objectSize, objectsLimit,
		p.chunkPoolInvokePrepareNewChunk,
		p.chunkPoolInvokeReleaseChunkBytes12)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithBytes12) chunkPoolInvokePrepareNewChunk(uChunk uintptr) {
	if p.prepareNewObjectFunc != nil {
		p.prepareNewObjectFunc(uChunk)
	}
}

func (p *HKVTableWithBytes12) chunkPoolInvokeReleaseChunkBytes12() {
	var (
		sharedIndex     uint32
		shared          *map[[12]byte]HKVTableObjectUPtrWithBytes12
		sharedRWMutex   *sync.RWMutex
		objKey          [12]byte
		uObject         HKVTableObjectUPtrWithBytes12
		uReleaseTargetK [12]byte
		uReleaseTarget  HKVTableObjectUPtrWithBytes12
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

func (p *HKVTableWithBytes12) allocObjectWithBytes12WithReadAcquire(objKey [12]byte) HKVTableObjectUPtrWithBytes12 {
	var uObject = HKVTableObjectUPtrWithBytes12(p.chunkPool.AllocRawChunk())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTableWithBytes12) checkObject(v HKVTableObjectUPtrWithBytes12, objKey [12]byte) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithBytes12) MustGetObjectWithReadAcquire(objKey [12]byte) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrWithBytes12 = 0
		shared        *map[[12]byte]HKVTableObjectUPtrWithBytes12
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedWithBytes12(objKey)
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
		uNewObject        HKVTableObjectUPtrWithBytes12 = p.allocObjectWithBytes12WithReadAcquire(objKey)
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

func (p *HKVTableWithBytes12) TryGetObjectWithReadAcquire(objKey [12]byte) uintptr {
	var (
		uObject       HKVTableObjectUPtrWithBytes12 = 0
		shared        *map[[12]byte]HKVTableObjectUPtrWithBytes12
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithBytes12(objKey)
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

func (p *HKVTableWithBytes12) DeleteObject(objKey [12]byte) {
	var (
		uObject       HKVTableObjectUPtrWithBytes12
		shared        *map[[12]byte]HKVTableObjectUPtrWithBytes12
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithBytes12(objKey)
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
