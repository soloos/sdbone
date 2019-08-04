package sdboned

import (
	"soloos/common/soloosbase"
	"soloos/sdbone/sdbone"
)

type SDBONED struct {
	options   Options
	SoloOSEnv soloosbase.SoloOSEnv
	SDBOne    sdbone.SDBOne
}

func (p *SDBONED) Init(options Options) error {
	var err error
	p.options = options
	err = p.SoloOSEnv.InitWithSNet("")
	if err != nil {
		return err
	}

	p.SDBOne.SoloOSEnv = &p.SoloOSEnv

	return nil
}

func (p *SDBONED) Start() error {
	return nil
}
