package sdboned

import (
	"soloos/common/soloosbase"
)

type SDBONED struct {
	options   Options
	SoloOSEnv soloosbase.SoloOSEnv
}

func (p *SDBONED) Init(options Options) error {
	var err error
	p.options = options
	err = p.SoloOSEnv.InitWithSNet("")
	if err != nil {
		return err
	}

	return nil
}

func (p *SDBONED) Start() error {
	return nil
}
