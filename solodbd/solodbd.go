package solodbd

import (
	"soloos/common/soloosbase"
	"soloos/solodb/solodb"
)

type SolodbD struct {
	options   Options
	SoloosEnv soloosbase.SoloosEnv
	Solodb    solodb.Solodb
}

func (p *SolodbD) Init(options Options) error {
	var err error
	p.options = options
	err = p.SoloosEnv.InitWithSNet("")
	if err != nil {
		return err
	}

	p.Solodb.SoloosEnv = &p.SoloosEnv

	return nil
}

func (p *SolodbD) Start() error {
	return nil
}
