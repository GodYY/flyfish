package node_sql

import (
	"github.com/sniperHW/flyfish/codec/pb"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/kendynet"
	"sync"
	"sync/atomic"
	"teacher/node/common/config"
	"teacher/node/common/logger"

	protocol "github.com/sniperHW/flyfish/proto"
)

var (
	listener    *net.Listener
	clientCount int64
	sessions    sync.Map
)

func Start(conf *config.Config, idx int) error {
	var err error

	initConfig(conf, idx)

	if err := initDBMeta(); err != nil {
		return err
	}
	logger.Logger().Infoln("load db-meta successfully.")

	initMessageHandler()
	registerMessageHandlers()
	initMessageRoutine()

	if err := startListen(); err != nil {
		return err
	}

	return nil
}

func registerMessageHandlers() {
	registerMessageHandler(uint16(protocol.CmdType_Get), get)
}

func startListen() error {
	var err error

	config := getNodeConfig()
	if listener, err = net.NewListener("tcp", config.ExternalAddr, verifyLogin); err != nil {
		return err
	}

	go func() {
		err := listener.Serve(func(session kendynet.StreamSession, compress bool) {
			go func() {
				session.SetRecvTimeout(protocol.PingTime * 2)
				session.SetSendQueueSize(10000)

				//只有配置了压缩开启同时客户端支持压缩才开启通信压缩
				session.SetReceiver(net.NewReceiver(pb.GetNamespace("request"), compress))
				session.SetEncoder(net.NewEncoder(pb.GetNamespace("response"), compress))

				session.SetCloseCallBack(onSessionClosed)
				onNewSession(session)

				if err := session.Start(func(event *kendynet.Event) {
					if event.EventType == kendynet.EventTypeError {
						event.Session.Close(event.Data.(error).Error(), 0)
					} else {
						msg := event.Data.(*net.Message)
						dispatchMessage(session, msg.GetCmd(), msg)
					}
				}); err != nil {
					logger.Errorf("session start error: %s", err)
				}
			}()
		})

		if err != nil {
			logger.Logger().Errorf("serve error: %s\n", err.Error())
		}

		logger.Infoln("listen stop.")
	}()

	logger.Infof("start listen on %s.", config.ExternalAddr)

	return nil
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func onNewSession(session kendynet.StreamSession) {
	atomic.AddInt64(&clientCount, 1)
	session.SetUserData(newCliConn(session))
	sessions.Store(session, session)
}

func onSessionClosed(session kendynet.StreamSession, reason string) {
	if u := session.GetUserData(); nil != u {
		switch u.(type) {
		case *cliConn:
			u.(*cliConn).clear()
		}
	}
	sessions.Delete(session)
	atomic.AddInt64(&clientCount, -1)
}
