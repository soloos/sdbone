package offheap

import (
	"soloos/common/util"
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockChunkPool struct {
	driver        *MockOffheapDriver
	chunks        map[int32]ChunkUintptr
	offheapDriver *OffheapDriver
	chunkPool     ChunkPool
}

func (p *MockChunkPool) Init(chunks map[int32]ChunkUintptr, offheapDriver *OffheapDriver,
	chunkSize int, chunksLimit int32) error {
	var err error
	p.chunks = chunks
	p.offheapDriver = offheapDriver
	err = p.offheapDriver.InitChunkPool(&p.chunkPool,
		chunkSize, chunksLimit,
		p.ChunkPoolInvokePrepareNewChunk, p.ChunkPoolInvokeReleaseChunk)
	if err != nil {
		return err
	}

	return nil
}

func (p *MockChunkPool) takeChunkForRelease() ChunkUintptr {
	for _, uChunk := range p.chunks {
		return uChunk
	}
	return 0x00
}

func (p *MockChunkPool) ChunkPoolInvokePrepareNewChunk(uChunk ChunkUintptr) {
}

func (p *MockChunkPool) ChunkPoolInvokeReleaseChunk() {
	uChunk := p.takeChunkForRelease()
	pChunk := uChunk.Ptr()
	delete(p.chunks, pChunk.ID)
	p.chunkPool.ReleaseChunk(uChunk)
	return
}

func (p *MockChunkPool) AllocChunk() ChunkUintptr {
	uChunk := p.chunkPool.AllocChunk()
	p.chunks[uChunk.Ptr().ID] = uChunk
	return uChunk
}

func BenchmarkChunkPool(b *testing.B) {
	var (
		offheapDriver OffheapDriver
		mockChunkPool MockChunkPool
	)

	util.AssertErrIsNil(offheapDriver.Init())
	util.AssertErrIsNil(mockChunkPool.Init(make(map[int32]ChunkUintptr), &offheapDriver, 10, 1024))
	for n := 0; n < b.N; n++ {
		mockChunkPool.AllocChunk()
	}
}

func TestChunkPool(t *testing.T) {
	var (
		offheapDriver OffheapDriver
		mockChunkPool MockChunkPool
		uChunk        ChunkUintptr
	)

	assert.NoError(t, offheapDriver.Init())
	assert.NoError(t, mockChunkPool.Init(make(map[int32]ChunkUintptr), &offheapDriver, 1024, 1024))
	uChunk = mockChunkPool.chunkPool.AllocChunk()
	assert.NotNil(t, uChunk)

	mockChunkPool.chunkPool.ReleaseChunk(uChunk)
}
