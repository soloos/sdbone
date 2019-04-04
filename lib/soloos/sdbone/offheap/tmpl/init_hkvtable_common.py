#!/usr/bin/env python2
# -*- coding: utf-8 -*-

ret = ''

ret += '''
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
	name           string
	objectSize     int
	objectsLimit   int32
	chunkPool      RawChunkPool
	// chunkPool      ChunkPool
	sharedCount    uint32
	sharedRWMutexs []sync.RWMutex

	prepareNewObjectFunc    HKVTableInvokePrepareNewObject
	beforeReleaseObjectFunc HKVTableInvokeBeforeReleaseObject
}
'''

row = '''
func (p *HKVTableCommon) GetSharedWithMagicKeyName(k MagicKeyType) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.sharedCount)
}
'''

ret += row.replace("MagicKeyName", "String").replace("MagicKeyType", "string")
ret += row.replace("MagicKeyName", "Bytes12").replace("MagicKeyType", "[12]byte")
ret += row.replace("MagicKeyName", "Bytes64").replace("MagicKeyType", "[64]byte")

row = '''
func (p *HKVTableCommon) GetSharedWithMagicKeyName(k MagicKeyType) int {
	return int(k % (MagicKeyType(p.sharedCount)))
}
'''
ret += row.replace("MagicKeyName", "Int32").replace("MagicKeyType", "int32")
ret += row.replace("MagicKeyName", "Int64").replace("MagicKeyType", "int64")

print ret
