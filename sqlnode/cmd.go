package node_sql

import (
	"github.com/sniperHW/flyfish/proto"
	"time"
)

type cmd interface {
	seqNo() int64
	uniKey() string
	isCancel() bool
	isTimeout() bool
	makeSqlTask() sqlTask
	reply(errCode int32, fields map[string]*proto.Field, version int64)
}

type cmdBase struct {
	conn *cliConn
	//msg  *net.Message
	sqNo             int64
	uKey             string
	table            string
	key              string
	processDeadline  time.Time
	responseDeadline time.Time
}

func newCmdBase(conn *cliConn, sqNo int64, unikey, table, key string, procDeadline, respDeadline time.Time) cmdBase {
	return cmdBase{
		conn:             conn,
		sqNo:             sqNo,
		uKey:             unikey,
		table:            table,
		key:              key,
		processDeadline:  procDeadline,
		responseDeadline: respDeadline,
	}
}

//func newCmdBase(conn *cliConn, msg *net.Message) cmdBase {
//	c := cmdBase{
//		conn: conn,
//		msg:  msg,
//	}
//
//	c.processDeadline, c.responseDeadline = getDeadline(msg.GetHead().Timeout)
//
//	return c
//}

func (c *cmdBase) seqNo() int64 {
	//return c.msg.GetHead().Seqno
	return c.sqNo
}

func (c *cmdBase) uniKey() string {
	//return c.msg.GetHead().UniKey
	return c.uKey
}

//func (c *cmdBase) getTableKey() (table, key string) {
//	table, key = c.msg.GetHead().SplitUniKey()
//	return
//}

func (c *cmdBase) isCancel() bool {
	return !c.conn.isCmdExist(c.seqNo())
}

func (c *cmdBase) isTimeout() bool {
	return c.processDeadline.After(time.Now())
}

func (c *cmdBase) beforeReply() bool {
	if !c.isCancel() {
		c.conn.remCmdBySeqNo(c.seqNo())

		return !c.isTimeout()
	}
}
