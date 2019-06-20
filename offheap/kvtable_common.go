package offheap

import "sync"

type KVTableAfterSetNewObj func()
type KVTableInvokePrepareNewObject func(u uintptr)
type KVTableInvokeBeforeReleaseObject func(u uintptr)

// KVTableObjectPoolObject
// user -> MustGetKVTableObjectWithReadAcquire -> allocObjectFromObjectPool ->
//      offheap.BlockPool.AllocBlock ->
//      BlockPoolAssistant.ObjectPoolInvokeReleaseObject ->
//      takeBlockForRelease -> beforeReleaseBlock -> releaseObjectToObjectPool ->
//      BlockPoolAssistant.ReleaseBlock -> user
// user -> MustGetKVTableObjectWithReadAcquire -> offheap.BlockPool.AllocBlock ->
//      BlockPoolAssistant.ObjectPoolInvokePrepareNewObject ->

// inited by script

type KVTableCommon struct {
	name          string
	objectSize    int
	objectsLimit  int32
	objectPool    RawObjectPool
	shardCount    uint32
	shardRWMutexs []sync.RWMutex

	prepareNewObjectFunc    KVTableInvokePrepareNewObject
	beforeReleaseObjectFunc KVTableInvokeBeforeReleaseObject
}

func (p *KVTableCommon) GetShardWithString(k string) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.shardCount)
}

func (p *KVTableCommon) GetShardWithBytes12(k [12]byte) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.shardCount)
}

func (p *KVTableCommon) GetShardWithBytes64(k [64]byte) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.shardCount)
}

func (p *KVTableCommon) GetShardWithBytes68(k [68]byte) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.shardCount)
}

func (p *KVTableCommon) GetShardWithInt32(k int32) int {
	return int(k % (int32(p.shardCount)))
}

func (p *KVTableCommon) GetShardWithInt64(k int64) int {
	return int(k % (int64(p.shardCount)))
}

func (p *KVTableCommon) GetShardWithUint64(k uint64) int {
	return int(k % (uint64(p.shardCount)))
}
