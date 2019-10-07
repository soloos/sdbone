package solodb

import (
	"soloos/common/iron"
	"soloos/common/log"
	"soloos/common/snet"
	"soloos/common/solodbapitypes"
	"time"
)

func (p *Solodb) SetHeartBeatServers(heartBeatServerOptionsArr []solodbapitypes.HeartBeatServerOptions) error {
	p.HeartBeatServerOptionsArr = heartBeatServerOptionsArr
	return nil
}

func (p *Solodb) doHeartBeat(options solodbapitypes.HeartBeatServerOptions) {
	var (
		heartBeat solodbapitypes.SolodbHeartBeat
		webret    iron.ApiOutputResult
		peer      snet.Peer
		urlPath   string
		err       error
	)

	heartBeat.SrpcPeerID = p.SrpcPeer.PeerID().Str()
	heartBeat.WebPeerID = p.WebPeer.PeerID().Str()

	for {
		peer, err = p.SoloosEnv.SNetDriver.GetPeer(options.PeerID)
		urlPath = peer.AddressStr() + "/Api/SDB/Solodb/HeartBeat"
		if err != nil {
			log.Error("Solodb HeartBeat post json error, urlPath:", urlPath, ", err:", err)
			goto HEARTBEAT_DONE
		}

		err = iron.PostJSON(urlPath, heartBeat, &webret)
		if err != nil {
			log.Error("Solodb HeartBeat post json(decode) error, urlPath:", urlPath, ", err:", err)
			goto HEARTBEAT_DONE
		}
		log.Info("Solodb heartbeat message:", webret)

	HEARTBEAT_DONE:
		time.Sleep(time.Duration(options.DurationMS) * time.Millisecond)
	}
}

func (p *Solodb) StartHeartBeat() error {
	for _, options := range p.HeartBeatServerOptionsArr {
		go p.doHeartBeat(options)
	}
	return nil
}
