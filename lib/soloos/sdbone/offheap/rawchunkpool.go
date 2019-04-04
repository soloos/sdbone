package offheap

import (
	"math"
	"sync"
	"sync/atomic"
)

type RawChunkPoolInvokePrepareNewRawChunk func(uRawChunk uintptr)
type RawChunkPoolInvokeReleaseRawChunk func()

// RawChunkPool
// user -> AllocRawChunk -> mallocRawChunk -> user
// user -> AllocRawChunk -> RawChunkPoolAssistant.RawChunkPoolInvokeReleaseRawChunk -> ReleaseRawChunk -> user
type RawChunkPool struct {
	ID int64

	rawChunkSize   uintptr
	rawChunksLimit int32

	prepareNewRawChunkFunc RawChunkPoolInvokePrepareNewRawChunk
	releaseRawChunkFunc    RawChunkPoolInvokeReleaseRawChunk

	perMmapBytesSize int
	currentMmapBytes *mmapbytes
	mmapBytesList    []*mmapbytes

	maxRawChunkID      int64
	rawChunksMutex     sync.Mutex
	activeRawChunksNum int32
	pool               NoGCUintptrPool
}

func (p *RawChunkPool) Init(id int64, rawChunkSize int, rawChunksLimit int32,
	prepareNewRawChunkFunc RawChunkPoolInvokePrepareNewRawChunk,
	releaseRawChunkFunc RawChunkPoolInvokeReleaseRawChunk) error {
	var (
		err error
	)

	p.ID = id
	p.rawChunkSize = uintptr(rawChunkSize)
	p.rawChunksLimit = rawChunksLimit
	if p.rawChunksLimit == -1 {
		p.perMmapBytesSize = int(1024 * int(p.rawChunkSize))
	} else {
		p.perMmapBytesSize = int(math.Ceil(float64(p.rawChunksLimit)/float64(16))) * int(p.rawChunkSize)
	}
	p.prepareNewRawChunkFunc = prepareNewRawChunkFunc
	p.releaseRawChunkFunc = releaseRawChunkFunc

	err = p.growMmapBytesList()
	if err != nil {
		return err
	}

	p.activeRawChunksNum = 0
	p.pool.New = p.mallocRawChunk

	return nil
}

func (p *RawChunkPool) growMmapBytesList() error {
	mmapBytes, err := AllocMmapBytes(int(p.perMmapBytesSize))
	if err != nil {
		return err
	}
	p.mmapBytesList = append(p.mmapBytesList, &mmapBytes)
	p.currentMmapBytes = p.mmapBytesList[len(p.mmapBytesList)-1]

	return nil
}

func (p *RawChunkPool) mallocRawChunk() uintptr {
	var (
		uRawChunk        uintptr
		currentMmapBytes *mmapbytes
		end              uintptr
		err              error
	)

	// step1 grow mem if need
	currentMmapBytes = p.currentMmapBytes
	end = atomic.AddUintptr(&currentMmapBytes.addrStart, p.rawChunkSize)
	if end > currentMmapBytes.addrEnd {
		p.rawChunksMutex.Lock()
		currentMmapBytes = p.currentMmapBytes
		end = atomic.AddUintptr(&currentMmapBytes.addrStart, p.rawChunkSize)
		if end < currentMmapBytes.addrEnd {
			p.rawChunksMutex.Unlock()
			goto STEP1_DONE
		}

		err = p.growMmapBytesList()
		if err != nil {
			p.rawChunksMutex.Unlock()
			goto STEP1_DONE
		}

		currentMmapBytes = p.currentMmapBytes
		end = currentMmapBytes.addrStart + p.rawChunkSize
		currentMmapBytes.addrStart = end
		p.rawChunksMutex.Unlock()
	}
STEP1_DONE:

	// step2 alloc mem for chunk
	if err == nil {
		// get chunk address
		uRawChunk = (uintptr)(end - p.rawChunkSize)
	}

	if err != nil {
		panic("malloc chunk error")
	}

	if p.prepareNewRawChunkFunc != nil {
		p.prepareNewRawChunkFunc(uRawChunk)
	}
	return uintptr(uRawChunk)
}

func (p *RawChunkPool) AllocRawChunk() uintptr {
	if p.rawChunksLimit == -1 {
		return p.pool.Get()
	}

	// assert p.rawChunkReleaser != nil
	if atomic.AddInt32(&p.activeRawChunksNum, 1) < p.rawChunksLimit {
		return p.pool.Get()
	}

	for p.activeRawChunksNum > p.rawChunksLimit {
		p.releaseRawChunkFunc()
	}

	return p.pool.Get()
}

func (p *RawChunkPool) ReleaseRawChunk(chunk uintptr) {
	atomic.AddInt32(&p.activeRawChunksNum, -1)
	p.pool.Put(uintptr(chunk))
}
