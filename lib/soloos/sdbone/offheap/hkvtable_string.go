package offheap

import (
	"sync"
	"unsafe"
)

// inited by script

type HKVTableObjectUPtrWithString uintptr

func (u HKVTableObjectUPtrWithString) Ptr() *HKVTableObjectWithString {
	return (*HKVTableObjectWithString)(unsafe.Pointer(u))
}

type HKVTableObjectWithString struct {
	ID string
	HSharedPointer
}

// Heavy Key-Value table
type HKVTableWithString struct {
	HKVTableCommon
	shareds []map[string]HKVTableObjectUPtrWithString
}

func (p *OffheapDriver) CreateHKVTableWithString(name string,
	objectSize int, objectsLimit int32, sharedCount uint32,
	prepareNewObjectFunc HKVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject,
) (*HKVTableWithString, error) {
	var (
		kvTable = new(HKVTableWithString)
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

func (p *HKVTableWithString) Init(name string,
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

func (p *HKVTableWithString) Name() string {
	return p.name
}

func (p *HKVTableWithString) prepareShareds(objectSize int, objectsLimit int32) error {
	var (
		sharedIndex uint32
		err         error
	)
	p.shareds = make([]map[string]HKVTableObjectUPtrWithString, p.sharedCount)
	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		p.shareds[sharedIndex] = make(map[string]HKVTableObjectUPtrWithString)
	}

	err = p.chunkPool.Init(-1, objectSize, objectsLimit,
		p.chunkPoolInvokePrepareNewChunk,
		p.chunkPoolInvokeReleaseChunkString)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithString) chunkPoolInvokePrepareNewChunk(uChunk uintptr) {
	if p.prepareNewObjectFunc != nil {
		p.prepareNewObjectFunc(uChunk)
	}
}

func (p *HKVTableWithString) chunkPoolInvokeReleaseChunkString() {
	var (
		sharedIndex     uint32
		shared          *map[string]HKVTableObjectUPtrWithString
		sharedRWMutex   *sync.RWMutex
		objKey          string
		uObject         HKVTableObjectUPtrWithString
		uReleaseTargetK string
		uReleaseTarget  HKVTableObjectUPtrWithString
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

func (p *HKVTableWithString) allocObjectWithStringWithReadAcquire(objKey string) HKVTableObjectUPtrWithString {
	var uObject = HKVTableObjectUPtrWithString(p.chunkPool.AllocRawChunk())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTableWithString) checkObject(v HKVTableObjectUPtrWithString, objKey string) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithString) TryGetObjectWithReadAcquire(objKey string) uintptr {
	var (
		uObject       HKVTableObjectUPtrWithString = 0
		shared        *map[string]HKVTableObjectUPtrWithString
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithString(objKey)
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

func (p *HKVTableWithString) MustGetObjectWithReadAcquire(objKey string) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrWithString = 0
		shared        *map[string]HKVTableObjectUPtrWithString
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedWithString(objKey)
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
		uNewObject        = p.allocObjectWithStringWithReadAcquire(objKey)
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

func (p *HKVTableWithString) DeleteObject(objKey string) {
	var (
		uObject       HKVTableObjectUPtrWithString
		shared        *map[string]HKVTableObjectUPtrWithString
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithString(objKey)
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
