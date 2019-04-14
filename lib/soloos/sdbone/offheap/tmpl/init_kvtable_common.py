#!/usr/bin/env python2
# -*- coding: utf-8 -*-

ret = ''

ret += '''
package offheap

import "sync"

type KVTableInvokePrepareNewObject func(v uintptr)
type KVTableInvokeBeforeReleaseObject func(v uintptr)

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
	name           string
	objectSize     int
	objectsLimit   int32
	objectPool      RawObjectPool
	shardCount    uint32
	shardRWMutexs []sync.RWMutex

	prepareNewObjectFunc    KVTableInvokePrepareNewObject
	beforeReleaseObjectFunc KVTableInvokeBeforeReleaseObject
}
'''

row = '''
func (p *KVTableCommon) GetShardWithMagicKeyName(k MagicKeyType) int {
	hash := uint32(2166136261)
	const prime32 = uint32(16777619)
	for i := 0; i < len(k); i++ {
		hash *= prime32
		hash ^= uint32(k[i])
	}
	return int(hash % p.shardCount)
}
'''

ret += row.replace("MagicKeyName", "String").replace("MagicKeyType", "string")
ret += row.replace("MagicKeyName", "Bytes12").replace("MagicKeyType", "[12]byte")
ret += row.replace("MagicKeyName", "Bytes64").replace("MagicKeyType", "[64]byte")
ret += row.replace("MagicKeyName", "Bytes68").replace("MagicKeyType", "[68]byte")

row = '''
func (p *KVTableCommon) GetShardWithMagicKeyName(k MagicKeyType) int {
	return int(k % (MagicKeyType(p.shardCount)))
}
'''
ret += row.replace("MagicKeyName", "Int32").replace("MagicKeyType", "int32")
ret += row.replace("MagicKeyName", "Int64").replace("MagicKeyType", "int64")
ret += row.replace("MagicKeyName", "Uint64").replace("MagicKeyType", "uint64")

print ret
