package solodb

import (
	"soloos/common/iron"
	"soloos/common/solodbapitypes"
	"soloos/common/snet"
	"soloos/common/snettypes"
	"soloos/common/soloosbase"
)

type Solodb struct {
	SoloOSEnv *soloosbase.SoloOSEnv
	SRPCPeer  snettypes.Peer
	WebPeer   snettypes.Peer

	HeartBeatServerOptionsArr []solodbapitypes.HeartBeatServerOptions
	SRPCServer                snet.SRPCServer
	WebServer                 iron.Server
	ServerDriver              iron.ServerDriver
}

var _ = iron.IServer(&Solodb{})

func (p *Solodb) ServerName() string {
	return "SoloOS.Solodb"
}

func (p *Solodb) Serve() error {
	var err error

	err = p.StartHeartBeat()
	if err != nil {
		return err
	}

	err = p.ServerDriver.Serve()
	if err != nil {
		return err
	}

	return nil
}

func (p *Solodb) Close() error {
	return p.ServerDriver.Close()
}
