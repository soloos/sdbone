package solodb

import (
	"soloos/common/iron"
	"soloos/common/snet"
	"soloos/common/snettypes"
	"soloos/common/solodbapitypes"
	"soloos/common/soloosbase"
)

type Solodb struct {
	SoloosEnv *soloosbase.SoloosEnv
	SrpcPeer  snettypes.Peer
	WebPeer   snettypes.Peer

	HeartBeatServerOptionsArr []solodbapitypes.HeartBeatServerOptions
	SrpcServer                snet.SrpcServer
	WebServer                 iron.Server
	ServerDriver              iron.ServerDriver
}

var _ = iron.IServer(&Solodb{})

func (p *Solodb) ServerName() string {
	return "Soloos.Solodb"
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
