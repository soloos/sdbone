package offheap

import "sync/atomic"

var (
	DefaultOffheapDriver OffheapDriver
)

func init() {
	var err error
	err = DefaultOffheapDriver.Init()
	if err != nil {
		panic(err)
	}
}

type OffheapDriver struct {
	maxTableID int64

	chunkPools    map[int64]*ChunkPool
	rawChunkPools map[int64]*RawChunkPool
}

func (p *OffheapDriver) Init() error {
	p.chunkPools = make(map[int64]*ChunkPool)
	p.rawChunkPools = make(map[int64]*RawChunkPool)
	return nil
}

func (p *OffheapDriver) AllocTableID() int64 {
	return atomic.AddInt64(&p.maxTableID, 1)
}

func InitRawObjectPool(pool *RawObjectPool,
	structSize int, rawChunksLimit int32,
	prepareNewRawChunkFunc RawChunkPoolInvokePrepareNewRawChunk,
	releaseRawChunkFunc RawChunkPoolInvokeReleaseRawChunk) error {

	return DefaultOffheapDriver.InitRawObjectPool(pool,
		structSize, rawChunksLimit,
		prepareNewRawChunkFunc,
		releaseRawChunkFunc)
}

func InitChunkPool(pool *ChunkPool, chunkSize int, chunksLimit int32,
	prepareNewChunkFunc ChunkPoolInvokePrepareNewChunk,
	releaseChunkFunc ChunkPoolInvokeReleaseChunk) error {

	return DefaultOffheapDriver.InitChunkPool(pool,
		chunkSize, chunksLimit,
		prepareNewChunkFunc,
		releaseChunkFunc)
}
