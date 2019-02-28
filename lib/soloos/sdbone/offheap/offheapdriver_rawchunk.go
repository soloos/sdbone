package offheap

func (p *OffheapDriver) SetRawChunkPool(rawChunkPool *RawChunkPool) {
	p.rawChunkPools[rawChunkPool.ID] = rawChunkPool
}

func (p *OffheapDriver) GetRawChunkPool(poolid int64) *RawChunkPool {
	return p.rawChunkPools[poolid]
}
