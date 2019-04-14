package offheap

import "sync/atomic"

var (
	DefaultOffheapDriver OffheapDriver
)

func init() {
	var err error
	err = DefaultOffheapDriver.Init()
	if err != nil {
		panic(err)
	}
}

type OffheapDriver struct {
	maxTableID int64

	rawObjectPools map[int64]*RawObjectPool
}

func (p *OffheapDriver) Init() error {
	p.rawObjectPools = make(map[int64]*RawObjectPool)
	return nil
}

func (p *OffheapDriver) AllocTableID() int64 {
	return atomic.AddInt64(&p.maxTableID, 1)
}
