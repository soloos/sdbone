package offheap

import (
	"math"
	"sync"
	"sync/atomic"
)

type RawObjectPoolInvokePrepareNewRawObject func(uRawObject uintptr)
type RawObjectPoolInvokeReleaseRawObject func()

// RawObjectPool
// user -> AllocRawObject -> mallocRawObject -> user
// user -> AllocRawObject -> RawObjectPoolAssistant.RawObjectPoolInvokeReleaseRawObject -> ReleaseRawObject -> user
type RawObjectPool struct {
	rawObjectSize   uintptr
	rawObjectsLimit int32

	prepareNewRawObjectFunc RawObjectPoolInvokePrepareNewRawObject
	releaseRawObjectFunc    RawObjectPoolInvokeReleaseRawObject

	perMmapBytesSize int
	currentMmapBytes *mmapbytes
	mmapBytesList    []*mmapbytes

	rawObjectsMutex     sync.Mutex
	activeRawObjectsNum int32
	pool                NoGCUintptrPool
}

func (p *RawObjectPool) Init(rawObjectSize int, rawObjectsLimit int32,
	prepareNewRawObjectFunc RawObjectPoolInvokePrepareNewRawObject,
	releaseRawObjectFunc RawObjectPoolInvokeReleaseRawObject) error {
	var (
		err error
	)

	p.rawObjectSize = uintptr(rawObjectSize)
	p.rawObjectsLimit = rawObjectsLimit
	if p.rawObjectsLimit == -1 {
		p.perMmapBytesSize = int(1024 * int(p.rawObjectSize))
	} else {
		p.perMmapBytesSize = int(math.Ceil(float64(p.rawObjectsLimit)/float64(16))) * int(p.rawObjectSize)
	}
	p.prepareNewRawObjectFunc = prepareNewRawObjectFunc
	p.releaseRawObjectFunc = releaseRawObjectFunc

	err = p.growMmapBytesList()
	if err != nil {
		return err
	}

	p.activeRawObjectsNum = 0
	p.pool.New = p.mallocRawObject

	return nil
}

func (p *RawObjectPool) growMmapBytesList() error {
	mmapBytes, err := AllocMmapBytes(int(p.perMmapBytesSize))
	if err != nil {
		return err
	}
	p.mmapBytesList = append(p.mmapBytesList, &mmapBytes)
	p.currentMmapBytes = p.mmapBytesList[len(p.mmapBytesList)-1]

	return nil
}

func (p *RawObjectPool) mallocRawObject() uintptr {
	var (
		uRawObject       uintptr
		currentMmapBytes *mmapbytes
		end              uintptr
		err              error
	)

	// step1 grow mem if need
	currentMmapBytes = p.currentMmapBytes
	end = atomic.AddUintptr(&currentMmapBytes.addrStart, p.rawObjectSize)
	if end > currentMmapBytes.addrEnd {
		p.rawObjectsMutex.Lock()
		currentMmapBytes = p.currentMmapBytes
		end = atomic.AddUintptr(&currentMmapBytes.addrStart, p.rawObjectSize)
		if end < currentMmapBytes.addrEnd {
			p.rawObjectsMutex.Unlock()
			goto STEP1_DONE
		}

		err = p.growMmapBytesList()
		if err != nil {
			p.rawObjectsMutex.Unlock()
			goto STEP1_DONE
		}

		currentMmapBytes = p.currentMmapBytes
		end = currentMmapBytes.addrStart + p.rawObjectSize
		currentMmapBytes.addrStart = end
		p.rawObjectsMutex.Unlock()
	}
STEP1_DONE:

	// step2 alloc mem for object
	if err == nil {
		// get object address
		uRawObject = (uintptr)(end - p.rawObjectSize)
	}

	if err != nil {
		panic("malloc object error")
	}

	if p.prepareNewRawObjectFunc != nil {
		p.prepareNewRawObjectFunc(uRawObject)
	}
	return uintptr(uRawObject)
}

func (p *RawObjectPool) AllocRawObject() uintptr {
	if p.rawObjectsLimit == -1 {
		return p.pool.Get()
	}

	// assert p.rawObjectReleaser != nil
	if atomic.AddInt32(&p.activeRawObjectsNum, 1) < p.rawObjectsLimit {
		return p.pool.Get()
	}

	for p.activeRawObjectsNum > p.rawObjectsLimit {
		p.releaseRawObjectFunc()
	}

	return p.pool.Get()
}

func (p *RawObjectPool) ReleaseRawObject(object uintptr) {
	atomic.AddInt32(&p.activeRawObjectsNum, -1)
	p.pool.Put(uintptr(object))
}
