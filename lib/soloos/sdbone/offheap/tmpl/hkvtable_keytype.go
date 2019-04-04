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
	HKVTableCommon
	shareds []map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
}

func (p *OffheapDriver) CreateHKVTableWithMagicKeyName(name string,
	objectSize int, objectsLimit int32, sharedCount uint32,
	prepareNewObjectFunc HKVTableInvokePrepareNewObject,
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject,
) (*HKVTableWithMagicKeyName, error) {
	var (
		kvTable = new(HKVTableWithMagicKeyName)
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

func (p *HKVTableWithMagicKeyName) Init(name string,
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

func (p *HKVTableWithMagicKeyName) Name() string {
	return p.name
}

func (p *HKVTableWithMagicKeyName) prepareShareds(objectSize int, objectsLimit int32) error {
	var (
		sharedIndex uint32
		err         error
	)
	p.shareds = make([]map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName, p.sharedCount)
	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		p.shareds[sharedIndex] = make(map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName)
	}

	err = p.chunkPool.Init(-1, objectSize, objectsLimit,
		p.chunkPoolInvokePrepareNewChunk,
		p.chunkPoolInvokeReleaseChunkMagicKeyName)
	if err != nil {
		return err
	}

	return nil
}

func (p *HKVTableWithMagicKeyName) chunkPoolInvokePrepareNewChunk(uChunk uintptr) {
	if p.prepareNewObjectFunc != nil {
		p.prepareNewObjectFunc(uChunk)
	}
}

func (p *HKVTableWithMagicKeyName) chunkPoolInvokeReleaseChunkMagicKeyName() {
	var (
		sharedIndex     uint32
		shared          *map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
		sharedRWMutex   *sync.RWMutex
		objKey          MagicKeyType
		uObject         HKVTableObjectUPtrWithMagicKeyName
		uReleaseTargetK MagicKeyType
		uReleaseTarget  HKVTableObjectUPtrWithMagicKeyName
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

func (p *HKVTableWithMagicKeyName) allocObjectWithMagicKeyNameWithReadAcquire(objKey MagicKeyType) HKVTableObjectUPtrWithMagicKeyName {
	var uObject = HKVTableObjectUPtrWithMagicKeyName(p.chunkPool.AllocRawChunk())
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().ID = objKey
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTableWithMagicKeyName) checkObject(v HKVTableObjectUPtrWithMagicKeyName, objKey MagicKeyType) bool {
	return v.Ptr().ID == objKey && v.Ptr().IsInited()
}

func (p *HKVTableWithMagicKeyName) MustGetObjectWithReadAcquire(objKey MagicKeyType) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrWithMagicKeyName = 0
		shared        *map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedWithMagicKeyName(objKey)
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
		uNewObject        HKVTableObjectUPtrWithMagicKeyName = p.allocObjectWithMagicKeyNameWithReadAcquire(objKey)
		isNewObjectSetted bool                               = false
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

func (p *HKVTableWithMagicKeyName) TryGetObjectWithReadAcquire(objKey MagicKeyType) uintptr {
	var (
		uObject       HKVTableObjectUPtrWithMagicKeyName = 0
		shared        *map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithMagicKeyName(objKey)
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

func (p *HKVTableWithMagicKeyName) DeleteObject(objKey MagicKeyType) {
	var (
		uObject       HKVTableObjectUPtrWithMagicKeyName
		shared        *map[MagicKeyType]HKVTableObjectUPtrWithMagicKeyName
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedWithMagicKeyName(objKey)
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
