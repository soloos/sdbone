package offheap

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunkMaskMergeIncludeNeighbour(t *testing.T) {
	var chunkMask ChunkMask

	chunkMask.Reset()
	chunkMask.MergeIncludeNeighbour(3, 3)
	chunkMask.MergeIncludeNeighbour(3, 3)
	chunkMask.MergeIncludeNeighbour(10, 20)
	chunkMask.MergeIncludeNeighbour(21, 69)
	chunkMask.MergeIncludeNeighbour(69, 70)
	chunkMask.MergeIncludeNeighbour(70, 70)
	chunkMask.MergeIncludeNeighbour(9, 9)
	chunkMask.MergeIncludeNeighbour(8, 29)
	chunkMask.MergeIncludeNeighbour(100, 300)
	chunkMask.MergeIncludeNeighbour(600, 800)
	chunkMask.MergeIncludeNeighbour(200, 700)
	assert.Equal(t, 3, chunkMask.MaskArrayLen)
	assert.Equal(t, (ChunkMaskEntry{3, 3}), chunkMask.MaskArray[0])
	assert.Equal(t, (ChunkMaskEntry{8, 70}), chunkMask.MaskArray[1])
	assert.Equal(t, (ChunkMaskEntry{100, 800}), chunkMask.MaskArray[2])

	chunkMask.MergeIncludeNeighbour(2, 70000)
	assert.Equal(t, 1, chunkMask.MaskArrayLen)
	assert.Equal(t, (ChunkMaskEntry{2, 70000}), chunkMask.MaskArray[0])

	chunkMask.Reset()
	chunkMask.MergeIncludeNeighbour(0, 10)
	chunkMask.MergeIncludeNeighbour(0, 20)
	chunkMask.MergeIncludeNeighbour(0, 30)
	chunkMask.MergeIncludeNeighbour(0, 100)
	chunkMask.MergeIncludeNeighbour(0, 200)
	assert.Equal(t, 1, chunkMask.MaskArrayLen)
	assert.Equal(t, (ChunkMaskEntry{0, 200}), chunkMask.MaskArray[0])
}
