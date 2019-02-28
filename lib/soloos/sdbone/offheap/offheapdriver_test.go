package offheap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type MockOffheapDriver struct {
	offheapDriver  *OffheapDriver
	mockChunkPools map[int32]*MockChunkPool
	chunks         map[int32]ChunkUintptr
}

func (p *MockOffheapDriver) Init(offheapDriver *OffheapDriver) error {
	p.offheapDriver = offheapDriver
	p.offheapDriver.Init()
	p.mockChunkPools = make(map[int32]*MockChunkPool)
	p.chunks = make(map[int32]ChunkUintptr)
	return nil
}

func (p *MockOffheapDriver) Put(chunk ChunkUintptr) {
	p.chunks[chunk.Ptr().ID] = chunk
}

func (p *MockOffheapDriver) InitChunkPool(chunkSize int, chunksLimit int32) error {
	var err error

	mockChunkPool := new(MockChunkPool)
	err = mockChunkPool.Init(p.chunks, p.offheapDriver, chunkSize, chunksLimit)
	if err != nil {
		return err
	}

	p.mockChunkPools[int32(chunkSize)] = mockChunkPool
	return nil
}

func MakeMockOffheapDriver(t *testing.T, chunksSizes []int, chunksLimit int32) *MockOffheapDriver {
	var (
		mockOffheapDriver MockOffheapDriver
	)

	chunkDrver := new(OffheapDriver)
	assert.NoError(t, chunkDrver.Init())
	assert.NoError(t, mockOffheapDriver.Init(chunkDrver))

	for _, chunkSize := range chunksSizes {
		mockOffheapDriver.InitChunkPool(chunkSize, chunksLimit)
	}

	return &mockOffheapDriver
}

func TestOffheapDriverAllocChunk(t *testing.T) {
	var chunksLimit int32 = 20
	mockOffheapDriver := MakeMockOffheapDriver(t, []int{2, 4}, chunksLimit)

	var chunk ChunkUintptr
	var i int32
	for i = 0; i <= chunksLimit; i++ {
		chunk = mockOffheapDriver.mockChunkPools[2].AllocChunk()
		mockOffheapDriver.Put(chunk)
		assert.NotNil(t, chunk.Ptr())
		assert.NotNil(t, chunk.Ptr().Data)
	}

	for i = 0; i < chunksLimit; i++ {
		mockOffheapDriver.mockChunkPools[2].ChunkPoolInvokeReleaseChunk()
	}
}
