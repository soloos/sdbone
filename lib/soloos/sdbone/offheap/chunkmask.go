package offheap

import (
	"sync"
	"unsafe"
)

const (
	ChunkMaskStructSize = unsafe.Sizeof(ChunkMask{})
)

type ChunkMaskEntry struct {
	Offset int
	End    int
}

type ChunkMaskUintptr uintptr

func (u ChunkMaskUintptr) Ptr() *ChunkMask {
	return (*ChunkMask)(unsafe.Pointer(u))
}

type ChunkMask struct {
	MergeElementRWMutex sync.RWMutex
	MaskArrayLen        int
	MaskArray           [MaskArrayElementsLimit]ChunkMaskEntry
}

func (p *ChunkMask) Reset() {
	p.MaskArrayLen = 0
}

// doMergeIncludeNeighbour 将一个 Mask 并入 maskArray 中的某个元素
// Mask定义为 [offset, end)
// maskArray定义为 [[offset0,end0], [offset1,end1], ...]
// 该merge包括临近元素
//
// param:
//			ignoreIndex int 		需要跳过检查的元素
//									该参数比较特殊，ResFileObject.appendNeedFlushDiskMaskOnlyForWriteData中有用到
//			offset 		int 		Mask.Offset
//			end			int 		Mask.End
//			maskArray	[][2]int	[[offset0,end0], [offset1,end1], ...]
// return:
//			int -1表示 maskArray 中没有元素可容纳该Mask，>=0则表示合并到 maskArray 中相应的key
func (p *ChunkMask) doMergeIncludeNeighbour(ignoreIndex, offset, end int) int {
	var (
		mergeIndex = -1
		v          ChunkMaskEntry
		k          int
	)

	for k = 0; k < p.MaskArrayLen; k++ {
		if k == ignoreIndex {
			continue
		}

		v = p.MaskArray[k]
		if offset == v.End+1 {
			//   offset
			//     |......
			// ---+
			p.MaskArray[k].End = offset
			v.End = offset
		}

		if end == v.Offset-1 {
			//	  end
			// ....|
			//      +-----
			p.MaskArray[k].Offset = end
			v.Offset = end
		}

		switch {
		case v.Offset <= offset && offset <= v.End:
			//    offset     end
			//  |...|...|----...
			//  +-------+
			if end > v.End {
				//   offset   end
				//  |..|..|----|
				// 	+-----+
				p.MaskArray[k].End = end
			} else {
				// offset   end
				//   |..|----|..|
				//   +----------+
			}
			mergeIndex = k

		case offset < v.Offset && end >= v.Offset:
			//  offset 	   end
			//    |------|..|..|..|
			// 	         +-----+
			p.MaskArray[k].Offset = offset
			if end > v.End {
				//  offset 	 	     end
				//    |---------------|
				// 	      +-------+
				p.MaskArray[k].End = end
			}
			mergeIndex = k

		case offset == end:
			if v.Offset <= offset && offset <= v.End {
				mergeIndex = k
			}
		}

	}

	return mergeIndex
}

// MergeIncludeNeighbour 将一个Mask并入 maskArray 中的某个元素
// 循环调用 _mergeMaskIntoMaskArrayIncludeNeighbour
// 该merge包括临近元素
// return
// isMergeEventHappened 	bool
// isSucess 				bool 如果超过 MaskArray 元素超过上限，则返回 false，反之返回 true
func (p *ChunkMask) MergeIncludeNeighbour(offset, end int) (isMergeEventHappened, isSucess bool) {
	p.MergeElementRWMutex.RLock()

	var (
		mergeIndex         = -1
		previousMergeIndex = -1
		limit              int
	)

	mergeIndex = p.doMergeIncludeNeighbour(mergeIndex, offset, end)
	if -1 == mergeIndex {
		// 如果无法合并入 maskArray 中某一个元素，则为 maskArray 创建新元素
		if p.MaskArrayLen >= len(p.MaskArray) {
			isMergeEventHappened = false
			isSucess = false
			goto MERGE_DONE
		}

		p.MaskArray[p.MaskArrayLen].Offset = offset
		p.MaskArray[p.MaskArrayLen].End = end
		p.MaskArrayLen++
		isMergeEventHappened = false
		isSucess = true
		goto MERGE_DONE
	}

	previousMergeIndex = mergeIndex

	// 将范围相接的块两两合并成一个
	// 一直尝试合并，直至无法合并，则退出循环
	for {
		// 由于之前的合并结束后 maskArray[previousMergeIndex] 会发生变化，所以有可能可以再做一次合并动作
		// 该合并为 maskArray[previousMergeIndex] 合并到其他 mask 中
		// 所以需要令 _mergeMaskIntoMaskArrayIncludeNeighbour 中 ignoreIndex 为previousMergeIndex
		mergeIndex = p.doMergeIncludeNeighbour(previousMergeIndex,
			p.MaskArray[previousMergeIndex].Offset,
			p.MaskArray[previousMergeIndex].End)

		if -1 == mergeIndex {
			// 没有可合并的 mask 则退出循环
			break

		} else {
			// maskArray[previousMergeIndex] 可被合并到 maskArray 中另一个 mask 中
			// 所以删除 maskArray[previousMergeIndex]
			limit = p.MaskArrayLen - 1
			for i := previousMergeIndex; i < limit; i++ {
				p.MaskArray[i] = p.MaskArray[i+1]
			}
			p.MaskArrayLen--
			previousMergeIndex = mergeIndex
		}
	}

	isMergeEventHappened = true
	isSucess = true

MERGE_DONE:
	p.MergeElementRWMutex.RUnlock()
	return
}

func (p *ChunkMask) Contains(offset, end int) bool {
	for i := 0; i < p.MaskArrayLen; i++ {
		if offset >= p.MaskArray[i].Offset &&
			end <= p.MaskArray[i].End {
			return true
		}
	}
	return false
}

func (p *ChunkMask) Set(offset, end int) {
	p.MaskArrayLen = 1
	p.MaskArray[0].Offset = offset
	p.MaskArray[0].End = end
}
