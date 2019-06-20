package offheap

type Bitmap struct {
	words []uint64
}

func (p *Bitmap) Has(pos int32) bool {
	word, bit := int(pos>>6), uint(pos%64)
	return word < len(p.words) && (p.words[word]&(1<<bit)) != 0
}

func (p *Bitmap) Reset() {
	for i, _ := range p.words {
		p.words[i] = 0
	}
}

func (p *Bitmap) Set(pos int32) {
	word, bit := int(pos>>6), uint(pos%64)
	for word >= len(p.words) {
		p.words = append(p.words, 0)
	}
	p.words[word] |= 1 << bit
}

func (p *Bitmap) UnSet(pos int32) {
	word, bit := int(pos>>6), uint(pos%64)
	for word >= len(p.words) {
		p.words = append(p.words, 0)
	}
	p.words[word] &= ^(1 << bit)
}

func (p *Bitmap) Expand(pos int32) {
	word := int(pos >> 6)
	for word >= len(p.words) {
		p.words = append(p.words, 0)
	}
}

func (p *Bitmap) UnsafeSet(pos int32) {
	p.words[int(pos>>6)] |= 1 << uint(pos%64)
}

func (p *Bitmap) UnsafeUnSet(pos int32) {
	p.words[int(pos>>6)] &= ^(1 << uint(pos%64))
}
