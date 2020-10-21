package node_sql

import (
	"github.com/golang/protobuf/proto"
	"github.com/sniperHW/flyfish/net"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"teacher/node/common/logger"
	"time"
)

type message struct {
	conn *cliConn
	msg  *net.Message
}

var (
	cNetCmd chan *message
)

func initMessageRoutine() {
	cNetCmd = make(chan *message, 10000)

	go func() {
		for m := range cNetCmd {
			h := messageHandlers[m.msg.GetCmd()]
			// todo pcall?
			h(m.conn, m.msg)
		}
	}()
}

func pushMessage(r *message) {
	cNetCmd <- r
}

type messageHandler func(*cliConn, *net.Message)

var (
	messageHandlers map[uint16]messageHandler
)

func initMessageHandler() {
	messageHandlers = make(map[uint16]messageHandler)
}

func registerMessageHandler(cmd uint16, h messageHandler) {
	if _, ok := messageHandlers[cmd]; !ok {
		messageHandlers[cmd] = h
	}
}

func dispatchMessage(session kendynet.StreamSession, cmd uint16, msg *net.Message) {
	if nil != msg {
		switch cmd {
		case uint16(protocol.CmdType_Ping):
			session.Send(net.NewMessage(net.CommonHead{}, &protocol.PingResp{
				Timestamp: time.Now().UnixNano(),
			}))
		case uint16(protocol.CmdType_Cancel):
			cancel(this.kvnode, session.GetUserData().(*cliConn), msg)
		case uint16(protocol.CmdType_ReloadTableConf):
			reloadTableMeta(session.GetUserData().(*cliConn), msg)
		default:
			if _, ok := messageHandlers[cmd]; ok {
				//投递给线程池处理
				pushMessage(&message{
					conn: session.GetUserData().(*cliConn),
					msg:  msg,
				})
			} else {
				logger.Logger().Errorf("message(%d) handler not found.", msg.GetCmd())
			}
		}
	}
}

func newMessage(seqNo int64, errCode int32, pb proto.Message) *net.Message {
	return net.NewMessage(
		net.CommonHead{
			Seqno:   seqNo,
			ErrCode: errCode,
		},
		pb,
	)
}
