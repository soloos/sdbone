package offheap

import (
	"sync"
)

type RawObjectPool struct {
	rawChunkPool RawChunkPool
	RawObjects   sync.Map
}

func (p *RawObjectPool) Init(id int64, structSize int, rawChunksLimit int32,
	prepareNewRawChunkFunc RawChunkPoolInvokePrepareNewRawChunk,
	releaseRawChunkFunc RawChunkPoolInvokeReleaseRawChunk) error {
	var (
		err error
	)

	err = p.rawChunkPool.Init(id, structSize, rawChunksLimit, prepareNewRawChunkFunc, releaseRawChunkFunc)
	if err != nil {
		return err
	}

	return nil
}

func (p *RawObjectPool) AllocRawObject() uintptr {
	return p.rawChunkPool.AllocRawChunk()
}

func (p *RawObjectPool) ReleaseRawObjectByID(id interface{}) uintptr {
	retI, exists := p.RawObjects.Load(id)
	if exists {
		p.RawObjects.Delete(id)
		return retI.(uintptr)
	}
	return 0
}
func (p *RawObjectPool) ReleaseRawObject(uRawObject uintptr) {
	if uRawObject == 0 {
		return
	}
	p.rawChunkPool.ReleaseRawChunk(uRawObject)
}

// MustGetRawChunk get or init a rawChunk
// The last result is true if the value was loaded, false if alloc.
func (p *RawObjectPool) MustGetRawObject(id interface{}) (uintptr, bool) {
	var (
		retI       interface{}
		uRawObject uintptr
		exists     bool
	)
	retI, exists = p.RawObjects.Load(id)
	if exists {
		return retI.(uintptr), true
	}

	uRawObject = p.AllocRawObject()
	retI, exists = p.RawObjects.LoadOrStore(id, uRawObject)
	if exists {
		p.ReleaseRawObject(uRawObject)
		return retI.(uintptr), true
	}

	return uRawObject, false
}
