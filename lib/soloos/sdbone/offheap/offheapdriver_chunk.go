package offheap

func (p *OffheapDriver) InitChunkPool(pool *ChunkPool,
	chunkSize int, chunksLimit int32,
	prepareNewChunkFunc ChunkPoolInvokePrepareNewChunk,
	releaseChunkFunc ChunkPoolInvokeReleaseChunk) error {
	err := pool.Init(p.AllocTableID(), chunkSize, chunksLimit, prepareNewChunkFunc, releaseChunkFunc)
	if err != nil {
		return err
	}

	p.SetChunkPool(pool)
	return nil
}

func (p *OffheapDriver) SetChunkPool(chunkPool *ChunkPool) {
	p.chunkPools[chunkPool.ID] = chunkPool
}

func (p *OffheapDriver) GetChunkPool(poolid int64) *ChunkPool {
	return p.chunkPools[poolid]
}
