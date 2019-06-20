package main

import (
	"soloos/util/offheap"
)

type Env struct {
	options       Options
	offheapDriver *offheap.OffheapDriver
}

func (p *Env) Init(options Options) {
	p.options = options
	p.offheapDriver = &offheap.DefaultOffheapDriver
}

func (p *Env) Start() {
}
