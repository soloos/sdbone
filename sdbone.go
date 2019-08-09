package sdbone

import (
	"soloos/common/iron"
	"soloos/common/sdbapitypes"
	"soloos/common/snet"
	"soloos/common/snettypes"
	"soloos/common/soloosbase"
)

type SDBOne struct {
	SoloOSEnv *soloosbase.SoloOSEnv
	SRPCPeer  snettypes.Peer
	WebPeer   snettypes.Peer

	HeartBeatServerOptionsArr []sdbapitypes.HeartBeatServerOptions
	SRPCServer                snet.SRPCServer
	WebServer                 iron.Server
	ServerDriver              iron.ServerDriver
}

var _ = iron.IServer(&SDBOne{})

func (p *SDBOne) ServerName() string {
	return "SoloOS.SDBOne"
}

func (p *SDBOne) Serve() error {
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

func (p *SDBOne) Close() error {
	return p.ServerDriver.Close()
}
