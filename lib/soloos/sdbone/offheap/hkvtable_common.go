package offheap

import "sync"

type HKVTableInvokePrepareNewObject func(v uintptr)
type HKVTableInvokeBeforeReleaseObject func(v uintptr)

// HKVTableObjectPoolChunk
// user -> MustGetHKVTableObjectWithReadAcquire -> allocChunkFromChunkPool ->
//      offheap.BlockPool.AllocBlock ->
//      BlockPoolAssistant.ChunkPoolInvokeReleaseChunk ->
//      takeBlockForRelease -> beforeReleaseBlock -> releaseChunkToChunkPool ->
//      BlockPoolAssistant.ReleaseBlock -> user
// user -> MustGetHKVTableObjectWithReadAcquire -> offheap.BlockPool.AllocBlock ->
//      BlockPoolAssistant.ChunkPoolInvokePrepareNewChunk ->

// inited by script

type HKVTableCommon struct {
	name         string
	objectSize   int
	objectsLimit int32
	chunkPool    RawChunkPool
	// chunkPool      ChunkPool
	sharedCount    uint32
	sharedRWMutexs []sync.RWMutex

	prepareNewObjectFunc    HKVTableInvokePrepareNewObject
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject
}

func (p *HKVTableCommon) GetSharedWithString(k string) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.sharedCount)
}

func (p *HKVTableCommon) GetSharedWithBytes12(k [12]byte) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.sharedCount)
}

func (p *HKVTableCommon) GetSharedWithBytes64(k [64]byte) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.sharedCount)
}

func (p *HKVTableCommon) GetSharedWithInt32(k int32) int {
	return int(k % (int32(p.sharedCount)))
}

func (p *HKVTableCommon) GetSharedWithInt64(k int64) int {
	return int(k % (int64(p.sharedCount)))
}
