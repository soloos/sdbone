#!/usr/bin/env python2
# -*- coding: utf-8 -*-

ret = '''package offheap

import (
    "sync"
)

// HKVTableObjectPoolChunk
// user -> MustGetHKVTableObjectWithReadAcquire -> allocChunkFromChunkPool ->
//      offheap.BlockPool.AllocBlock ->
//      BlockPoolAssistant.ChunkPoolInvokeReleaseChunk ->
//      takeBlockForRelease -> beforeReleaseBlock -> releaseChunkToChunkPool ->
//      BlockPoolAssistant.ReleaseBlock -> user
// user -> MustGetHKVTableObjectWithReadAcquire -> offheap.BlockPool.AllocBlock ->
//      BlockPoolAssistant.ChunkPoolInvokePrepareNewChunk ->
'''

schemaKeyTypes = [
        {'keyName': 'String', 'keyType': 'string'},
        {'keyName': 'Int32', 'keyType': 'int32'},
        {'keyName': 'Int64', 'keyType': 'int64'},
        {'keyName': 'Bytes12', 'keyType': '[12]byte'},
        {'keyName': 'Bytes64', 'keyType': '[64]byte'},
        ]


ret += '''
func (p *HKVTable) prepareShareds(keyType string, objectSize int, objectsLimit int32) error {
    var (
        sharedIndex uint32
        err error
    )
    switch keyType {
'''

tmplRow = '''
    case "{keyName}":
        p.sharedsBy{keyName} = make([]map[{keyType}]HKVTableObjectUPtrBy{keyName}, p.sharedCount)
        for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {{
            p.sharedsBy{keyName}[sharedIndex] = make(map[{keyType}]HKVTableObjectUPtrBy{keyName})
        }}

        err = p.chunkPool.Init(-1, objectSize, objectsLimit,
            p.chunkPoolInvokePrepareNewChunk{keyName},
            p.chunkPoolInvokeReleaseChunk{keyName})
	    if err != nil {{
	    	return err
	    }}

        return nil
'''

for row in schemaKeyTypes:
    ret += tmplRow.format(keyName=row['keyName'], keyType=row['keyType'])

ret += '''
    }

    return ErrUnknownKeyType
}

'''

tmplRow = '''
func (p *HKVTable) GetSharedBy{keyName}(k {keyType}) int {{
    hash := uint32(2166136261)
    const prime32 = uint32(16777619)
    for i := 0; i < len(k); i++ {{
        hash *= prime32
        hash ^= uint32(k[i])
    }}
    return int(hash % p.sharedCount)
}}
'''
ret += tmplRow.format(keyName='String', keyType='string')
ret += tmplRow.format(keyName='Bytes12', keyType='[12]byte')
ret += tmplRow.format(keyName='Bytes64', keyType='[64]byte')

tmplRow = '''
func (p *HKVTable) GetSharedBy{keyName}(k {keyType}) int {{
    return int(k % ({keyType}(p.sharedCount)))
}}
'''
ret += tmplRow.format(keyName='Int32', keyType='int32')
ret += tmplRow.format(keyName='Int64', keyType='int64')


tmplRow = '''

func (p *HKVTable) chunkPoolInvokePrepareNewChunk{keyName}(uChunk uintptr) {{
}}

func (p *HKVTable) chunkPoolInvokeReleaseChunk{keyName}() {{
    var (
        sharedIndex uint32
        shared *map[{keyType}]HKVTableObjectUPtrBy{keyName}
        sharedRWMutex *sync.RWMutex
        k {keyType}
        uObject HKVTableObjectUPtrBy{keyName}
        uReleaseTargetK {keyType}
        uReleaseTarget HKVTableObjectUPtrBy{keyName}
    )

    for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {{
        shared = &p.sharedsBy{keyName}[sharedIndex]
        sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

        sharedRWMutex.RLock()
        for k, uObject = range *shared {{
            if uObject.Ptr().IsInited() == true &&
                uObject.Ptr().GetAccessor() > 0 {{
                    uReleaseTargetK = k
                    uReleaseTarget = uObject
                    goto FIND_STEP0_TARGET_DONE
            }}
        }}
        sharedRWMutex.RUnlock()
    }}
FIND_STEP0_TARGET_DONE:

    if uReleaseTarget == 0 {{
        for sharedIndex = 0; sharedIndex < p.sharedCount; sharedIndex++ {{
            shared = &p.sharedsBy{keyName}[sharedIndex]
            sharedRWMutex = &p.sharedRWMutexs[sharedIndex]

            sharedRWMutex.RLock()
            for k, uObject = range *shared {{
                if uObject.Ptr().IsInited() == true {{
                        uReleaseTargetK = k
                        uReleaseTarget = uObject
                        goto FIND_STEP1_TARGET_DONE
                }}
            }}
            sharedRWMutex.RUnlock()
        }}
    }}
FIND_STEP1_TARGET_DONE:

    if uReleaseTarget != 0 {{
        p.DeleteObjectBy{keyName}(uReleaseTargetK)
    }}
}}

func (p *HKVTable) allocObjectBy{keyName}WithReadAcquire(k {keyType}) HKVTableObjectUPtrBy{keyName} {{
    var uObject = HKVTableObjectUPtrBy{keyName}(p.chunkPool.AllocRawChunk())
    uObject.Ptr().Key = k
    uObject.Ptr().ReadAcquire()
    uObject.Ptr().CompleteInit()
    return uObject
}}

func (p *HKVTable) releaseObjectBy{keyName}(uObject HKVTableObjectUPtrBy{keyName}) {{
    uObject.Ptr().Reset()
    p.chunkPool.ReleaseRawChunk(uintptr(uObject))
}}

func (p *HKVTable) checkObjectBy{keyName}(v HKVTableObjectUPtrBy{keyName}, k {keyType}) bool {{
    return v.Ptr().HSharedPointer.Status != HSharedPointerUninited && v.Ptr().Key != k
}}

func (p *HKVTable) MustGetObjectBy{keyName}WithReadAcquire(k {keyType}) (uintptr, bool) {{
    var (
        uObject HKVTableObjectUPtrBy{keyName} = 0
        shared *map[{keyType}]HKVTableObjectUPtrBy{keyName}
        sharedRWMutex *sync.RWMutex
        loaded bool = false
    )

    {{
        sharedIndex := p.GetSharedBy{keyName}(k)
        shared = &p.sharedsBy{keyName}[sharedIndex]
        sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
    }}

    sharedRWMutex.RLock()
    uObject, loaded = (*shared)[k]
    sharedRWMutex.RUnlock()

    if uObject != 0 {{
        uObject.Ptr().HSharedPointer.ReadAcquire()
        if p.checkObjectBy{keyName}(uObject, k) == false {{
            uObject.Ptr().HSharedPointer.ReadRelease()
            uObject = 0
        }} else {{
            loaded = true
        }}
    }}
    
    if uObject != 0 {{
        return uintptr(uObject), loaded 
    }}

    var (
        uNewObject HKVTableObjectUPtrBy{keyName} = p.allocObjectBy{keyName}WithReadAcquire(k)
        isNewObjectSetted bool = false
    )

    for isNewObjectSetted == false && loaded == false {{
        sharedRWMutex.Lock()
        uObject, _ = (*shared)[k]
        if uObject == 0 {{
            uObject = uNewObject
            (*shared)[k] = uObject
            isNewObjectSetted = true
        }}
        sharedRWMutex.Unlock()

        if isNewObjectSetted == false {{
            if p.checkObjectBy{keyName}(uObject, k) {{
                loaded = true
            }} else {{
                uObject.Ptr().ReadRelease()
                uObject = 0
            }}
        }}
    }}

    if isNewObjectSetted == false {{
        p.releaseObjectBy{keyName}(uNewObject)
        uNewObject.Ptr().ReadRelease()
    }}

    return uintptr(uObject), loaded
}}

func (p *HKVTable) TryGetObjectBy{keyName}WithReadAcquire(k {keyType}) uintptr {{
    var (
        uObject HKVTableObjectUPtrBy{keyName} = 0
        shared *map[{keyType}]HKVTableObjectUPtrBy{keyName}
        sharedRWMutex *sync.RWMutex
    )

    {{
        sharedIndex := p.GetSharedBy{keyName}(k)
        shared = &p.sharedsBy{keyName}[sharedIndex]
        sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
    }}

    sharedRWMutex.RLock()
    uObject, _ = (*shared)[k]
    sharedRWMutex.RUnlock()

    if uObject != 0 {{
        uObject.Ptr().ReadAcquire()
        if p.checkObjectBy{keyName}(uObject, k) == false {{
            uObject.Ptr().ReadRelease()
            uObject = 0
        }}
    }}

    return uintptr(uObject)
}}

func (p *HKVTable) DeleteObjectBy{keyName}(k {keyType}) {{
    var (
        uObject HKVTableObjectUPtrBy{keyName}
        shared *map[{keyType}]HKVTableObjectUPtrBy{keyName}
        sharedRWMutex *sync.RWMutex
    )

    {{
        sharedIndex := p.GetSharedBy{keyName}(k)
        shared = &p.sharedsBy{keyName}[sharedIndex]
        sharedRWMutex = &p.sharedRWMutexs[sharedIndex]
    }}

    for {{
        sharedRWMutex.RLock()
        uObject, _ = (*shared)[k]
        sharedRWMutex.RUnlock()

        if uObject == 0 {{
            return
        }}

        uObject.Ptr().WriteAcquire()
        if p.checkObjectBy{keyName}(uObject, k) == false {{
            uObject.Ptr().WriteRelease()
            uObject = 0
            continue
        }}
    }}

    // assert uObject != 0

    for {{
        p.beforeReleaseObjectFunc(uintptr(uObject))

        if uObject.Ptr().IsShouldRelease() {{
            sharedRWMutex.Lock()
            delete(*shared, k)
            sharedRWMutex.Unlock()
            p.releaseObjectBy{keyName}(uObject)
            uObject.Ptr().WriteRelease()
            break
        }}
    }}
}}
'''
for row in schemaKeyTypes:
    ret += tmplRow.format(keyName=row['keyName'], keyType=row['keyType'])

print ret
