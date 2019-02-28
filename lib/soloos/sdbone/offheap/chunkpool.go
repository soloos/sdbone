package offheap

import (
	"math"
	"sync"
	"sync/atomic"
)

type ChunkPoolInvokePrepareNewChunk func(uChunk ChunkUintptr)
type ChunkPoolInvokeReleaseChunk func()

// ChunkPool
// user -> AllocChunk -> mallocChunk -> user
// user -> AllocChunk -> ChunkPoolAssistant.ChunkPoolInvokeReleaseChunk -> ReleaseChunk -> user
type ChunkPool struct {
	ID int64

	chunkSize   uintptr
	chunksLimit int32

	prepareNewChunkFunc ChunkPoolInvokePrepareNewChunk
	releaseChunkFunc    ChunkPoolInvokeReleaseChunk

	chunkWithStructSize uintptr
	perMmapBytesSize    int
	currentMmapBytes    *mmapbytes
	mmapBytesList       []*mmapbytes

	maxChunkID      int64
	chunksMutex     sync.Mutex
	activeChunksNum int32
	pool            NoGCUintptrPool
	chunks          map[uintptr]uintptr
}

func (p *ChunkPool) Init(id int64, chunkSize int, chunksLimit int32,
	prepareNewChunkFunc ChunkPoolInvokePrepareNewChunk,
	releaseChunkFunc ChunkPoolInvokeReleaseChunk) error {
	var (
		err error
	)

	p.ID = id
	p.chunkSize = uintptr(chunkSize)
	p.chunkWithStructSize = ChunkStructSize + p.chunkSize
	p.chunksLimit = chunksLimit
	if p.chunksLimit == -1 {
		p.perMmapBytesSize = int(1024 * int(p.chunkWithStructSize))
	} else {
		p.perMmapBytesSize = int(math.Ceil(float64(p.chunksLimit)/float64(16))) * int(p.chunkWithStructSize)
	}
	p.prepareNewChunkFunc = prepareNewChunkFunc
	p.releaseChunkFunc = releaseChunkFunc

	err = p.growMmapBytesList()
	if err != nil {
		return err
	}

	p.activeChunksNum = 0
	p.pool.New = p.mallocChunk

	return nil
}

func (p *ChunkPool) growMmapBytesList() error {
	mmapBytes, err := AllocMmapBytes(int(p.perMmapBytesSize))
	if err != nil {
		return err
	}
	p.mmapBytesList = append(p.mmapBytesList, &mmapBytes)
	p.currentMmapBytes = p.mmapBytesList[len(p.mmapBytesList)-1]

	return nil
}

func (p *ChunkPool) mallocChunk() uintptr {
	var (
		uChunk           ChunkUintptr
		currentMmapBytes *mmapbytes
		end              uintptr
		err              error
	)

	// step1 grow mem if need
	if err == nil {
		currentMmapBytes = p.currentMmapBytes
		end = atomic.AddUintptr(&currentMmapBytes.addrStart, p.chunkWithStructSize)
		if end > currentMmapBytes.addrEnd {
			p.chunksMutex.Lock()
			currentMmapBytes = p.currentMmapBytes
			end = atomic.AddUintptr(&currentMmapBytes.addrStart, p.chunkWithStructSize)
			if end < currentMmapBytes.addrEnd {
				p.chunksMutex.Unlock()
				goto STEP1_DONE
			}

			err = p.growMmapBytesList()
			if err != nil {
				p.chunksMutex.Unlock()
				goto STEP1_DONE
			}

			currentMmapBytes = p.currentMmapBytes
			end = currentMmapBytes.addrStart + p.chunkWithStructSize
			currentMmapBytes.addrStart = end
			p.chunksMutex.Unlock()
		}
	}
STEP1_DONE:

	// step2 alloc mem for chunk
	if err == nil {
		// get chunk address
		uChunk = (ChunkUintptr)(end - p.chunkWithStructSize)

		// save chunk in offheap.data
		uChunk.Ptr().ID = atomic.AddInt64(&p.maxChunkID, 1)
		uChunk.Ptr().Data = uintptr(uChunk) + ChunkStructSize
	}

	if err != nil {
		panic("malloc chunk error")
	}

	if p.prepareNewChunkFunc != nil {
		p.prepareNewChunkFunc(uChunk)
	}
	return uintptr(uChunk)
}

func (p *ChunkPool) AllocChunk() ChunkUintptr {
	if p.chunksLimit == -1 {
		return ChunkUintptr(p.pool.Get())
	}

	// assert p.chunkReleaser != nil
	if atomic.AddInt32(&p.activeChunksNum, 1) < p.chunksLimit {
		return ChunkUintptr(p.pool.Get())
	}

	for p.activeChunksNum > p.chunksLimit {
		p.releaseChunkFunc()
	}

	return ChunkUintptr(p.pool.Get())
}

func (p *ChunkPool) ReleaseChunk(chunk ChunkUintptr) {
	atomic.AddInt32(&p.activeChunksNum, -1)
	p.pool.Put(uintptr(chunk))
}
