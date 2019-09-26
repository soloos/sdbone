package solodbd

import (
	"soloos/common/soloosbase"
	"soloos/solodb/solodb"
)

type SOLODBD struct {
	options   Options
	SoloOSEnv soloosbase.SoloOSEnv
	Solodb    solodb.Solodb
}

func (p *SOLODBD) Init(options Options) error {
	var err error
	p.options = options
	err = p.SoloOSEnv.InitWithSNet("")
	if err != nil {
		return err
	}

	p.Solodb.SoloOSEnv = &p.SoloOSEnv

	return nil
}

func (p *SOLODBD) Start() error {
	return nil
}
