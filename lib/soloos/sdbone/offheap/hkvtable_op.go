package offheap

import (
	"sync"
)

// HKVTableObjectPoolChunk
// user -> MustGetHKVTableObjectWithReadAcquire -> allocChunkFromChunkPool ->
//      offheap.BlockPool.AllocBlock ->
//      BlockPoolAssistant.ChunkPoolInvokeReleaseChunk ->
//      takeBlockForRelease -> beforeReleaseBlock -> releaseChunkToChunkPool ->
//      BlockPoolAssistant.ReleaseBlock -> user
// user -> MustGetHKVTableObjectWithReadAcquire -> offheap.BlockPool.AllocBlock ->
//      BlockPoolAssistant.ChunkPoolInvokePrepareNewChunk ->

func (p *HKVTable) prepareShareds(keyType string, objectSize int, objectsLimit int32) error {
	var (
		sharedIndex uint32
		err         error
	)
	switch keyType {

	case "String":
		p.sharedsByString = make([]map[string]HKVTableObjectUPtrByString, p.sharedCount)
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			p.sharedsByString[sharedIndex] = make(map[string]HKVTableObjectUPtrByString)
		}

		err = p.chunkPool.Init(-1, objectSize, objectsLimit,
			p.chunkPoolInvokePrepareNewChunkString,
			p.chunkPoolInvokeReleaseChunkString)
		if err != nil {
			return err
		}

		return nil

	case "Int32":
		p.sharedsByInt32 = make([]map[int32]HKVTableObjectUPtrByInt32, p.sharedCount)
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			p.sharedsByInt32[sharedIndex] = make(map[int32]HKVTableObjectUPtrByInt32)
		}

		err = p.chunkPool.Init(-1, objectSize, objectsLimit,
			p.chunkPoolInvokePrepareNewChunkInt32,
			p.chunkPoolInvokeReleaseChunkInt32)
		if err != nil {
			return err
		}

		return nil

	case "Int64":
		p.sharedsByInt64 = make([]map[int64]HKVTableObjectUPtrByInt64, p.sharedCount)
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			p.sharedsByInt64[sharedIndex] = make(map[int64]HKVTableObjectUPtrByInt64)
		}

		err = p.chunkPool.Init(-1, objectSize, objectsLimit,
			p.chunkPoolInvokePrepareNewChunkInt64,
			p.chunkPoolInvokeReleaseChunkInt64)
		if err != nil {
			return err
		}

		return nil

	case "Bytes12":
		p.sharedsByBytes12 = make([]map[[12]byte]HKVTableObjectUPtrByBytes12, p.sharedCount)
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			p.sharedsByBytes12[sharedIndex] = make(map[[12]byte]HKVTableObjectUPtrByBytes12)
		}

		err = p.chunkPool.Init(-1, objectSize, objectsLimit,
			p.chunkPoolInvokePrepareNewChunkBytes12,
			p.chunkPoolInvokeReleaseChunkBytes12)
		if err != nil {
			return err
		}

		return nil

	case "Bytes64":
		p.sharedsByBytes64 = make([]map[[64]byte]HKVTableObjectUPtrByBytes64, p.sharedCount)
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			p.sharedsByBytes64[sharedIndex] = make(map[[64]byte]HKVTableObjectUPtrByBytes64)
		}

		err = p.chunkPool.Init(-1, objectSize, objectsLimit,
			p.chunkPoolInvokePrepareNewChunkBytes64,
			p.chunkPoolInvokeReleaseChunkBytes64)
		if err != nil {
			return err
		}

		return nil

	}

	return ErrUnknownKeyType
}

func (p *HKVTable) GetSharedByString(k string) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.sharedCount)
}

func (p *HKVTable) GetSharedByBytes12(k [12]byte) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.sharedCount)
}

func (p *HKVTable) GetSharedByBytes64(k [64]byte) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.sharedCount)
}

func (p *HKVTable) GetSharedByInt32(k int32) int {
	return int(k % (int32(p.sharedCount)))
}

func (p *HKVTable) GetSharedByInt64(k int64) int {
	return int(k % (int64(p.sharedCount)))
}

func (p *HKVTable) chunkPoolInvokePrepareNewChunkString(uChunk uintptr) {
}

func (p *HKVTable) chunkPoolInvokeReleaseChunkString() {
	var (
		sharedIndex     uint32
		shared          *map[string]HKVTableObjectUPtrByString
		sharedRWMutex   *sync.RWMutex
		k               string
		uObject         HKVTableObjectUPtrByString
		uReleaseTargetK string
		uReleaseTarget  HKVTableObjectUPtrByString
	)

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.sharedsByString[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for k, uObject = range *shared {
			if uObject.Ptr().IsInited() == true &&
				uObject.Ptr().GetAccessor() > 0 {
				uReleaseTargetK = k
				uReleaseTarget = uObject
				goto FIND_STEP0_TARGET_DONE
			}
		}
		sharedRWMutex.RUnlock()
	}
FIND_STEP0_TARGET_DONE:

	if uReleaseTarget == 0 {
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			shared = &p.sharedsByString[sharedIndex]
			sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

			sharedRWMutex.RLock()
			for k, uObject = range *shared {
				if uObject.Ptr().IsInited() == true {
					uReleaseTargetK = k
					uReleaseTarget = uObject
					goto FIND_STEP1_TARGET_DONE
				}
			}
			sharedRWMutex.RUnlock()
		}
	}
FIND_STEP1_TARGET_DONE:

	if uReleaseTarget != 0 {
		p.DeleteObjectByString(uReleaseTargetK)
	}
}

func (p *HKVTable) allocObjectByStringWithReadAcquire(k string) HKVTableObjectUPtrByString {
	var uObject = HKVTableObjectUPtrByString(p.chunkPool.AllocRawChunk())
	uObject.Ptr().Key = k
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTable) releaseObjectByString(uObject HKVTableObjectUPtrByString) {
	uObject.Ptr().Reset()
	p.chunkPool.ReleaseRawChunk(uintptr(uObject))
}

func (p *HKVTable) checkObjectByString(v HKVTableObjectUPtrByString, k string) bool {
	return v.Ptr().HSharedPointer.Status != HSharedPointerUninited && v.Ptr().Key != k
}

func (p *HKVTable) MustGetObjectByStringWithReadAcquire(k string) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrByString = 0
		shared        *map[string]HKVTableObjectUPtrByString
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedByString(k)
		shared = &p.sharedsByString[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, loaded = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().HSharedPointer.ReadAcquire()
		if p.checkObjectByString(uObject, k) == false {
			uObject.Ptr().HSharedPointer.ReadRelease()
			uObject = 0
		} else {
			loaded = true
		}
	}

	if uObject != 0 {
		return uintptr(uObject), loaded
	}

	var (
		uNewObject        HKVTableObjectUPtrByString = p.allocObjectByStringWithReadAcquire(k)
		isNewObjectSetted bool                       = false
	)

	for isNewObjectSetted == false && loaded == false {
		sharedRWMutex.Lock()
		uObject, _ = (*shared)[k]
		if uObject == 0 {
			uObject = uNewObject
			(*shared)[k] = uObject
			isNewObjectSetted = true
		}
		sharedRWMutex.Unlock()

		if isNewObjectSetted == false {
			if p.checkObjectByString(uObject, k) {
				loaded = true
			} else {
				uObject.Ptr().ReadRelease()
				uObject = 0
			}
		}
	}

	if isNewObjectSetted == false {
		p.releaseObjectByString(uNewObject)
		uNewObject.Ptr().ReadRelease()
	}

	return uintptr(uObject), loaded
}

func (p *HKVTable) TryGetObjectByStringWithReadAcquire(k string) uintptr {
	var (
		uObject       HKVTableObjectUPtrByString = 0
		shared        *map[string]HKVTableObjectUPtrByString
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByString(k)
		shared = &p.sharedsByString[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, _ = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().ReadAcquire()
		if p.checkObjectByString(uObject, k) == false {
			uObject.Ptr().ReadRelease()
			uObject = 0
		}
	}

	return uintptr(uObject)
}

func (p *HKVTable) DeleteObjectByString(k string) {
	var (
		uObject       HKVTableObjectUPtrByString
		shared        *map[string]HKVTableObjectUPtrByString
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByString(k)
		shared = &p.sharedsByString[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	for {
		sharedRWMutex.RLock()
		uObject, _ = (*shared)[k]
		sharedRWMutex.RUnlock()

		if uObject == 0 {
			return
		}

		uObject.Ptr().WriteAcquire()
		if p.checkObjectByString(uObject, k) == false {
			uObject.Ptr().WriteRelease()
			uObject = 0
			continue
		}
	}

	// assert uObject != 0

	for {
		p.beforeReleaseObjectFunc(uintptr(uObject))

		if uObject.Ptr().IsShouldRelease() {
			sharedRWMutex.Lock()
			delete(*shared, k)
			sharedRWMutex.Unlock()
			p.releaseObjectByString(uObject)
			uObject.Ptr().WriteRelease()
			break
		}
	}
}

func (p *HKVTable) chunkPoolInvokePrepareNewChunkInt32(uChunk uintptr) {
}

func (p *HKVTable) chunkPoolInvokeReleaseChunkInt32() {
	var (
		sharedIndex     uint32
		shared          *map[int32]HKVTableObjectUPtrByInt32
		sharedRWMutex   *sync.RWMutex
		k               int32
		uObject         HKVTableObjectUPtrByInt32
		uReleaseTargetK int32
		uReleaseTarget  HKVTableObjectUPtrByInt32
	)

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.sharedsByInt32[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for k, uObject = range *shared {
			if uObject.Ptr().IsInited() == true &&
				uObject.Ptr().GetAccessor() > 0 {
				uReleaseTargetK = k
				uReleaseTarget = uObject
				goto FIND_STEP0_TARGET_DONE
			}
		}
		sharedRWMutex.RUnlock()
	}
FIND_STEP0_TARGET_DONE:

	if uReleaseTarget == 0 {
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			shared = &p.sharedsByInt32[sharedIndex]
			sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

			sharedRWMutex.RLock()
			for k, uObject = range *shared {
				if uObject.Ptr().IsInited() == true {
					uReleaseTargetK = k
					uReleaseTarget = uObject
					goto FIND_STEP1_TARGET_DONE
				}
			}
			sharedRWMutex.RUnlock()
		}
	}
FIND_STEP1_TARGET_DONE:

	if uReleaseTarget != 0 {
		p.DeleteObjectByInt32(uReleaseTargetK)
	}
}

func (p *HKVTable) allocObjectByInt32WithReadAcquire(k int32) HKVTableObjectUPtrByInt32 {
	var uObject = HKVTableObjectUPtrByInt32(p.chunkPool.AllocRawChunk())
	uObject.Ptr().Key = k
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTable) releaseObjectByInt32(uObject HKVTableObjectUPtrByInt32) {
	uObject.Ptr().Reset()
	p.chunkPool.ReleaseRawChunk(uintptr(uObject))
}

func (p *HKVTable) checkObjectByInt32(v HKVTableObjectUPtrByInt32, k int32) bool {
	return v.Ptr().HSharedPointer.Status != HSharedPointerUninited && v.Ptr().Key != k
}

func (p *HKVTable) MustGetObjectByInt32WithReadAcquire(k int32) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrByInt32 = 0
		shared        *map[int32]HKVTableObjectUPtrByInt32
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedByInt32(k)
		shared = &p.sharedsByInt32[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, loaded = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().HSharedPointer.ReadAcquire()
		if p.checkObjectByInt32(uObject, k) == false {
			uObject.Ptr().HSharedPointer.ReadRelease()
			uObject = 0
		} else {
			loaded = true
		}
	}

	if uObject != 0 {
		return uintptr(uObject), loaded
	}

	var (
		uNewObject        HKVTableObjectUPtrByInt32 = p.allocObjectByInt32WithReadAcquire(k)
		isNewObjectSetted bool                      = false
	)

	for isNewObjectSetted == false && loaded == false {
		sharedRWMutex.Lock()
		uObject, _ = (*shared)[k]
		if uObject == 0 {
			uObject = uNewObject
			(*shared)[k] = uObject
			isNewObjectSetted = true
		}
		sharedRWMutex.Unlock()

		if isNewObjectSetted == false {
			if p.checkObjectByInt32(uObject, k) {
				loaded = true
			} else {
				uObject.Ptr().ReadRelease()
				uObject = 0
			}
		}
	}

	if isNewObjectSetted == false {
		p.releaseObjectByInt32(uNewObject)
		uNewObject.Ptr().ReadRelease()
	}

	return uintptr(uObject), loaded
}

func (p *HKVTable) TryGetObjectByInt32WithReadAcquire(k int32) uintptr {
	var (
		uObject       HKVTableObjectUPtrByInt32 = 0
		shared        *map[int32]HKVTableObjectUPtrByInt32
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByInt32(k)
		shared = &p.sharedsByInt32[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, _ = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().ReadAcquire()
		if p.checkObjectByInt32(uObject, k) == false {
			uObject.Ptr().ReadRelease()
			uObject = 0
		}
	}

	return uintptr(uObject)
}

func (p *HKVTable) DeleteObjectByInt32(k int32) {
	var (
		uObject       HKVTableObjectUPtrByInt32
		shared        *map[int32]HKVTableObjectUPtrByInt32
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByInt32(k)
		shared = &p.sharedsByInt32[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	for {
		sharedRWMutex.RLock()
		uObject, _ = (*shared)[k]
		sharedRWMutex.RUnlock()

		if uObject == 0 {
			return
		}

		uObject.Ptr().WriteAcquire()
		if p.checkObjectByInt32(uObject, k) == false {
			uObject.Ptr().WriteRelease()
			uObject = 0
			continue
		}
	}

	// assert uObject != 0

	for {
		p.beforeReleaseObjectFunc(uintptr(uObject))

		if uObject.Ptr().IsShouldRelease() {
			sharedRWMutex.Lock()
			delete(*shared, k)
			sharedRWMutex.Unlock()
			p.releaseObjectByInt32(uObject)
			uObject.Ptr().WriteRelease()
			break
		}
	}
}

func (p *HKVTable) chunkPoolInvokePrepareNewChunkInt64(uChunk uintptr) {
}

func (p *HKVTable) chunkPoolInvokeReleaseChunkInt64() {
	var (
		sharedIndex     uint32
		shared          *map[int64]HKVTableObjectUPtrByInt64
		sharedRWMutex   *sync.RWMutex
		k               int64
		uObject         HKVTableObjectUPtrByInt64
		uReleaseTargetK int64
		uReleaseTarget  HKVTableObjectUPtrByInt64
	)

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.sharedsByInt64[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for k, uObject = range *shared {
			if uObject.Ptr().IsInited() == true &&
				uObject.Ptr().GetAccessor() > 0 {
				uReleaseTargetK = k
				uReleaseTarget = uObject
				goto FIND_STEP0_TARGET_DONE
			}
		}
		sharedRWMutex.RUnlock()
	}
FIND_STEP0_TARGET_DONE:

	if uReleaseTarget == 0 {
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			shared = &p.sharedsByInt64[sharedIndex]
			sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

			sharedRWMutex.RLock()
			for k, uObject = range *shared {
				if uObject.Ptr().IsInited() == true {
					uReleaseTargetK = k
					uReleaseTarget = uObject
					goto FIND_STEP1_TARGET_DONE
				}
			}
			sharedRWMutex.RUnlock()
		}
	}
FIND_STEP1_TARGET_DONE:

	if uReleaseTarget != 0 {
		p.DeleteObjectByInt64(uReleaseTargetK)
	}
}

func (p *HKVTable) allocObjectByInt64WithReadAcquire(k int64) HKVTableObjectUPtrByInt64 {
	var uObject = HKVTableObjectUPtrByInt64(p.chunkPool.AllocRawChunk())
	uObject.Ptr().Key = k
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTable) releaseObjectByInt64(uObject HKVTableObjectUPtrByInt64) {
	uObject.Ptr().Reset()
	p.chunkPool.ReleaseRawChunk(uintptr(uObject))
}

func (p *HKVTable) checkObjectByInt64(v HKVTableObjectUPtrByInt64, k int64) bool {
	return v.Ptr().HSharedPointer.Status != HSharedPointerUninited && v.Ptr().Key != k
}

func (p *HKVTable) MustGetObjectByInt64WithReadAcquire(k int64) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrByInt64 = 0
		shared        *map[int64]HKVTableObjectUPtrByInt64
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedByInt64(k)
		shared = &p.sharedsByInt64[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, loaded = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().HSharedPointer.ReadAcquire()
		if p.checkObjectByInt64(uObject, k) == false {
			uObject.Ptr().HSharedPointer.ReadRelease()
			uObject = 0
		} else {
			loaded = true
		}
	}

	if uObject != 0 {
		return uintptr(uObject), loaded
	}

	var (
		uNewObject        HKVTableObjectUPtrByInt64 = p.allocObjectByInt64WithReadAcquire(k)
		isNewObjectSetted bool                      = false
	)

	for isNewObjectSetted == false && loaded == false {
		sharedRWMutex.Lock()
		uObject, _ = (*shared)[k]
		if uObject == 0 {
			uObject = uNewObject
			(*shared)[k] = uObject
			isNewObjectSetted = true
		}
		sharedRWMutex.Unlock()

		if isNewObjectSetted == false {
			if p.checkObjectByInt64(uObject, k) {
				loaded = true
			} else {
				uObject.Ptr().ReadRelease()
				uObject = 0
			}
		}
	}

	if isNewObjectSetted == false {
		p.releaseObjectByInt64(uNewObject)
		uNewObject.Ptr().ReadRelease()
	}

	return uintptr(uObject), loaded
}

func (p *HKVTable) TryGetObjectByInt64WithReadAcquire(k int64) uintptr {
	var (
		uObject       HKVTableObjectUPtrByInt64 = 0
		shared        *map[int64]HKVTableObjectUPtrByInt64
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByInt64(k)
		shared = &p.sharedsByInt64[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, _ = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().ReadAcquire()
		if p.checkObjectByInt64(uObject, k) == false {
			uObject.Ptr().ReadRelease()
			uObject = 0
		}
	}

	return uintptr(uObject)
}

func (p *HKVTable) DeleteObjectByInt64(k int64) {
	var (
		uObject       HKVTableObjectUPtrByInt64
		shared        *map[int64]HKVTableObjectUPtrByInt64
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByInt64(k)
		shared = &p.sharedsByInt64[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	for {
		sharedRWMutex.RLock()
		uObject, _ = (*shared)[k]
		sharedRWMutex.RUnlock()

		if uObject == 0 {
			return
		}

		uObject.Ptr().WriteAcquire()
		if p.checkObjectByInt64(uObject, k) == false {
			uObject.Ptr().WriteRelease()
			uObject = 0
			continue
		}
	}

	// assert uObject != 0

	for {
		p.beforeReleaseObjectFunc(uintptr(uObject))

		if uObject.Ptr().IsShouldRelease() {
			sharedRWMutex.Lock()
			delete(*shared, k)
			sharedRWMutex.Unlock()
			p.releaseObjectByInt64(uObject)
			uObject.Ptr().WriteRelease()
			break
		}
	}
}

func (p *HKVTable) chunkPoolInvokePrepareNewChunkBytes12(uChunk uintptr) {
}

func (p *HKVTable) chunkPoolInvokeReleaseChunkBytes12() {
	var (
		sharedIndex     uint32
		shared          *map[[12]byte]HKVTableObjectUPtrByBytes12
		sharedRWMutex   *sync.RWMutex
		k               [12]byte
		uObject         HKVTableObjectUPtrByBytes12
		uReleaseTargetK [12]byte
		uReleaseTarget  HKVTableObjectUPtrByBytes12
	)

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.sharedsByBytes12[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for k, uObject = range *shared {
			if uObject.Ptr().IsInited() == true &&
				uObject.Ptr().GetAccessor() > 0 {
				uReleaseTargetK = k
				uReleaseTarget = uObject
				goto FIND_STEP0_TARGET_DONE
			}
		}
		sharedRWMutex.RUnlock()
	}
FIND_STEP0_TARGET_DONE:

	if uReleaseTarget == 0 {
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			shared = &p.sharedsByBytes12[sharedIndex]
			sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

			sharedRWMutex.RLock()
			for k, uObject = range *shared {
				if uObject.Ptr().IsInited() == true {
					uReleaseTargetK = k
					uReleaseTarget = uObject
					goto FIND_STEP1_TARGET_DONE
				}
			}
			sharedRWMutex.RUnlock()
		}
	}
FIND_STEP1_TARGET_DONE:

	if uReleaseTarget != 0 {
		p.DeleteObjectByBytes12(uReleaseTargetK)
	}
}

func (p *HKVTable) allocObjectByBytes12WithReadAcquire(k [12]byte) HKVTableObjectUPtrByBytes12 {
	var uObject = HKVTableObjectUPtrByBytes12(p.chunkPool.AllocRawChunk())
	uObject.Ptr().Key = k
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTable) releaseObjectByBytes12(uObject HKVTableObjectUPtrByBytes12) {
	uObject.Ptr().Reset()
	p.chunkPool.ReleaseRawChunk(uintptr(uObject))
}

func (p *HKVTable) checkObjectByBytes12(v HKVTableObjectUPtrByBytes12, k [12]byte) bool {
	return v.Ptr().HSharedPointer.Status != HSharedPointerUninited && v.Ptr().Key != k
}

func (p *HKVTable) MustGetObjectByBytes12WithReadAcquire(k [12]byte) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrByBytes12 = 0
		shared        *map[[12]byte]HKVTableObjectUPtrByBytes12
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedByBytes12(k)
		shared = &p.sharedsByBytes12[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, loaded = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().HSharedPointer.ReadAcquire()
		if p.checkObjectByBytes12(uObject, k) == false {
			uObject.Ptr().HSharedPointer.ReadRelease()
			uObject = 0
		} else {
			loaded = true
		}
	}

	if uObject != 0 {
		return uintptr(uObject), loaded
	}

	var (
		uNewObject        HKVTableObjectUPtrByBytes12 = p.allocObjectByBytes12WithReadAcquire(k)
		isNewObjectSetted bool                        = false
	)

	for isNewObjectSetted == false && loaded == false {
		sharedRWMutex.Lock()
		uObject, _ = (*shared)[k]
		if uObject == 0 {
			uObject = uNewObject
			(*shared)[k] = uObject
			isNewObjectSetted = true
		}
		sharedRWMutex.Unlock()

		if isNewObjectSetted == false {
			if p.checkObjectByBytes12(uObject, k) {
				loaded = true
			} else {
				uObject.Ptr().ReadRelease()
				uObject = 0
			}
		}
	}

	if isNewObjectSetted == false {
		p.releaseObjectByBytes12(uNewObject)
		uNewObject.Ptr().ReadRelease()
	}

	return uintptr(uObject), loaded
}

func (p *HKVTable) TryGetObjectByBytes12WithReadAcquire(k [12]byte) uintptr {
	var (
		uObject       HKVTableObjectUPtrByBytes12 = 0
		shared        *map[[12]byte]HKVTableObjectUPtrByBytes12
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByBytes12(k)
		shared = &p.sharedsByBytes12[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, _ = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().ReadAcquire()
		if p.checkObjectByBytes12(uObject, k) == false {
			uObject.Ptr().ReadRelease()
			uObject = 0
		}
	}

	return uintptr(uObject)
}

func (p *HKVTable) DeleteObjectByBytes12(k [12]byte) {
	var (
		uObject       HKVTableObjectUPtrByBytes12
		shared        *map[[12]byte]HKVTableObjectUPtrByBytes12
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByBytes12(k)
		shared = &p.sharedsByBytes12[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	for {
		sharedRWMutex.RLock()
		uObject, _ = (*shared)[k]
		sharedRWMutex.RUnlock()

		if uObject == 0 {
			return
		}

		uObject.Ptr().WriteAcquire()
		if p.checkObjectByBytes12(uObject, k) == false {
			uObject.Ptr().WriteRelease()
			uObject = 0
			continue
		}
	}

	// assert uObject != 0

	for {
		p.beforeReleaseObjectFunc(uintptr(uObject))

		if uObject.Ptr().IsShouldRelease() {
			sharedRWMutex.Lock()
			delete(*shared, k)
			sharedRWMutex.Unlock()
			p.releaseObjectByBytes12(uObject)
			uObject.Ptr().WriteRelease()
			break
		}
	}
}

func (p *HKVTable) chunkPoolInvokePrepareNewChunkBytes64(uChunk uintptr) {
}

func (p *HKVTable) chunkPoolInvokeReleaseChunkBytes64() {
	var (
		sharedIndex     uint32
		shared          *map[[64]byte]HKVTableObjectUPtrByBytes64
		sharedRWMutex   *sync.RWMutex
		k               [64]byte
		uObject         HKVTableObjectUPtrByBytes64
		uReleaseTargetK [64]byte
		uReleaseTarget  HKVTableObjectUPtrByBytes64
	)

	for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
		shared = &p.sharedsByBytes64[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

		sharedRWMutex.RLock()
		for k, uObject = range *shared {
			if uObject.Ptr().IsInited() == true &&
				uObject.Ptr().GetAccessor() > 0 {
				uReleaseTargetK = k
				uReleaseTarget = uObject
				goto FIND_STEP0_TARGET_DONE
			}
		}
		sharedRWMutex.RUnlock()
	}
FIND_STEP0_TARGET_DONE:

	if uReleaseTarget == 0 {
		for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {
			shared = &p.sharedsByBytes64[sharedIndex]
			sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

			sharedRWMutex.RLock()
			for k, uObject = range *shared {
				if uObject.Ptr().IsInited() == true {
					uReleaseTargetK = k
					uReleaseTarget = uObject
					goto FIND_STEP1_TARGET_DONE
				}
			}
			sharedRWMutex.RUnlock()
		}
	}
FIND_STEP1_TARGET_DONE:

	if uReleaseTarget != 0 {
		p.DeleteObjectByBytes64(uReleaseTargetK)
	}
}

func (p *HKVTable) allocObjectByBytes64WithReadAcquire(k [64]byte) HKVTableObjectUPtrByBytes64 {
	var uObject = HKVTableObjectUPtrByBytes64(p.chunkPool.AllocRawChunk())
	uObject.Ptr().Key = k
	uObject.Ptr().ReadAcquire()
	uObject.Ptr().CompleteInit()
	return uObject
}

func (p *HKVTable) releaseObjectByBytes64(uObject HKVTableObjectUPtrByBytes64) {
	uObject.Ptr().Reset()
	p.chunkPool.ReleaseRawChunk(uintptr(uObject))
}

func (p *HKVTable) checkObjectByBytes64(v HKVTableObjectUPtrByBytes64, k [64]byte) bool {
	return v.Ptr().HSharedPointer.Status != HSharedPointerUninited && v.Ptr().Key != k
}

func (p *HKVTable) MustGetObjectByBytes64WithReadAcquire(k [64]byte) (uintptr, bool) {
	var (
		uObject       HKVTableObjectUPtrByBytes64 = 0
		shared        *map[[64]byte]HKVTableObjectUPtrByBytes64
		sharedRWMutex *sync.RWMutex
		loaded        bool = false
	)

	{
		sharedIndex := p.GetSharedByBytes64(k)
		shared = &p.sharedsByBytes64[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, loaded = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().HSharedPointer.ReadAcquire()
		if p.checkObjectByBytes64(uObject, k) == false {
			uObject.Ptr().HSharedPointer.ReadRelease()
			uObject = 0
		} else {
			loaded = true
		}
	}

	if uObject != 0 {
		return uintptr(uObject), loaded
	}

	var (
		uNewObject        HKVTableObjectUPtrByBytes64 = p.allocObjectByBytes64WithReadAcquire(k)
		isNewObjectSetted bool                        = false
	)

	for isNewObjectSetted == false && loaded == false {
		sharedRWMutex.Lock()
		uObject, _ = (*shared)[k]
		if uObject == 0 {
			uObject = uNewObject
			(*shared)[k] = uObject
			isNewObjectSetted = true
		}
		sharedRWMutex.Unlock()

		if isNewObjectSetted == false {
			if p.checkObjectByBytes64(uObject, k) {
				loaded = true
			} else {
				uObject.Ptr().ReadRelease()
				uObject = 0
			}
		}
	}

	if isNewObjectSetted == false {
		p.releaseObjectByBytes64(uNewObject)
		uNewObject.Ptr().ReadRelease()
	}

	return uintptr(uObject), loaded
}

func (p *HKVTable) TryGetObjectByBytes64WithReadAcquire(k [64]byte) uintptr {
	var (
		uObject       HKVTableObjectUPtrByBytes64 = 0
		shared        *map[[64]byte]HKVTableObjectUPtrByBytes64
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByBytes64(k)
		shared = &p.sharedsByBytes64[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	sharedRWMutex.RLock()
	uObject, _ = (*shared)[k]
	sharedRWMutex.RUnlock()

	if uObject != 0 {
		uObject.Ptr().ReadAcquire()
		if p.checkObjectByBytes64(uObject, k) == false {
			uObject.Ptr().ReadRelease()
			uObject = 0
		}
	}

	return uintptr(uObject)
}

func (p *HKVTable) DeleteObjectByBytes64(k [64]byte) {
	var (
		uObject       HKVTableObjectUPtrByBytes64
		shared        *map[[64]byte]HKVTableObjectUPtrByBytes64
		sharedRWMutex *sync.RWMutex
	)

	{
		sharedIndex := p.GetSharedByBytes64(k)
		shared = &p.sharedsByBytes64[sharedIndex]
		sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
	}

	for {
		sharedRWMutex.RLock()
		uObject, _ = (*shared)[k]
		sharedRWMutex.RUnlock()

		if uObject == 0 {
			return
		}

		uObject.Ptr().WriteAcquire()
		if p.checkObjectByBytes64(uObject, k) == false {
			uObject.Ptr().WriteRelease()
			uObject = 0
			continue
		}
	}

	// assert uObject != 0

	for {
		p.beforeReleaseObjectFunc(uintptr(uObject))

		if uObject.Ptr().IsShouldRelease() {
			sharedRWMutex.Lock()
			delete(*shared, k)
			sharedRWMutex.Unlock()
			p.releaseObjectByBytes64(uObject)
			uObject.Ptr().WriteRelease()
			break
		}
	}
}
