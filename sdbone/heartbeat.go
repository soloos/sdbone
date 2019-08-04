package sdbone

import (
	"soloos/common/iron"
	"soloos/common/log"
	"soloos/common/sdbapitypes"
	"soloos/common/snettypes"
	"time"
)

func (p *SDBOne) SetHeartBeatServers(heartBeatServerOptionsArr []sdbapitypes.HeartBeatServerOptions) error {
	p.HeartBeatServerOptionsArr = heartBeatServerOptionsArr
	return nil
}

func (p *SDBOne) doHeartBeat(options sdbapitypes.HeartBeatServerOptions) {
	var (
		heartBeat sdbapitypes.SDBOneHeartBeat
		webret    iron.ApiOutputResult
		peer      snettypes.Peer
		urlPath   string
		err       error
	)

	heartBeat.SRPCPeerID = p.SRPCPeer.PeerID().Str()
	heartBeat.WebPeerID = p.WebPeer.PeerID().Str()

	for {
		peer, err = p.SoloOSEnv.SNetDriver.GetPeer(options.PeerID)
		urlPath = peer.AddressStr() + "/Api/SDB/SDBOne/HeartBeat"
		if err != nil {
			log.Error("SDBOne HeartBeat post json error, urlPath:", urlPath, ", err:", err)
			goto HEARTBEAT_DONE
		}

		err = iron.PostJSON(urlPath, heartBeat, &webret)
		if err != nil {
			log.Error("SDBOne HeartBeat post json(decode) error, urlPath:", urlPath, ", err:", err)
			goto HEARTBEAT_DONE
		}
		log.Info("SDBOne heartbeat message:", webret)

	HEARTBEAT_DONE:
		time.Sleep(time.Duration(options.DurationMS) * time.Millisecond)
	}
}

func (p *SDBOne) StartHeartBeat() error {
	for _, options := range p.HeartBeatServerOptionsArr {
		go p.doHeartBeat(options)
	}
	return nil
}
