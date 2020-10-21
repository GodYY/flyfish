package node_sql

import (
	"database/sql"
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"strings"
	"teacher/node/common/logger"
)
import "github.com/sniperHW/flyfish/proto"

type cmdGet struct {
	cmdBase
	getAll  bool
	fields  []string
	version *int64
}

//func (c *cmdGet) getReq() *proto.GetReq {
//	return c.msg.GetData().(*proto.GetReq)
//}

func (c *cmdGet) makeSqlTask() sqlTask {
	tableFieldCount := getDBMeta().getTableMeta(c.table).getFieldCount()

	task := &sqlTaskGet{
		sqlTaskBase: newSqlTaskBase(c.uKey, c.table, c.key, tableFieldCount),
	}

	if c.getAll {
		task.getAll = true
	} else {
		task.getFields = make([]string, 0, tableFieldCount)
		for _, v := range c.fields {
			task.getFields = append(task.getFields, v)
			task.fields[v] = proto.PackField(v, nil)
		}
	}

	pVersion := c.version
	if pVersion != nil {
		task.version = *pVersion
		task.getWithVersion = true
		task.versionOp = "!="
	} else {
		task.version = 0
		task.getWithVersion = false
	}

	task.addCmd(c)

	return task
}

func (c *cmdGet) reply(errCode int32, fields map[string]*proto.Field, version int64) {
	if c.beforeReply() {
		var resp = &proto.GetResp{
			Version: version,
			Fields:  nil,
		}

		if errCode == errcode.ERR_OK {
			if c.version != nil && version == *c.version {
				errCode = errcode.ERR_RECORD_UNCHANGE
			} else {
				resp.Fields = make([]*proto.Field, len(c.fields))
				for i, v := range c.fields {
					if f := fields[v]; f == nil {
						logger.Logger().Errorf("get table(%s) key(%s) lost field(%s).", c.table, c.key, v)
					} else {
						resp.Fields[i] = f
					}
				}
			}
		}

		_ = c.conn.sendMessage(net.NewMessage(
			net.CommonHead{
				Seqno:   c.sqNo,
				ErrCode: errCode,
			},
			resp,
		))
	}
}

type sqlTaskGet struct {
	sqlTaskBase
	getAll         bool
	getFields      []string
	getWithVersion bool
	versionOp      string
}

func (t *sqlTaskGet) combine(c cmd) bool {
	cmd, ok := c.(*cmdGet)
	if !ok {
		return false
	}

	if c.uniKey() != t.uniKey {
		return false
	}

	if cmd.version == nil && t.getWithVersion {
		t.getWithVersion = false
	} else if cmd.version != nil && t.getWithVersion && *cmd.version < t.version {
		t.version = *cmd.version
		t.versionOp = ">="
	}

	if cmd.getAll {
		t.getAll = true
		t.getFields = nil
	} else if !t.getAll {
		for _, v := range cmd.fields {
			if _, ok := t.fields[v]; !ok {
				t.getFields = append(t.getFields, v)
				t.fields[v] = nil
			}
		}
	}

	t.addCmd(c)

	return true
}

func (t *sqlTaskGet) do(db *sqlx.DB) {
	t_meta := getDBMeta().getTableMeta(t.table)

	var (
		queryFields          []string
		queryFieldReceivers  []interface{}
		queryFieldConverters []fieldConverter
		queryFieldCount      int
		getFieldOffset       int
		versionIndex         int
	)

	if t.getAll {
		queryFields = t_meta.getAllFields()
		queryFieldReceivers = t_meta.getAllFieldReceivers()
		queryFieldConverters = t_meta.getAllFieldConverter()
		queryFieldCount = len(queryFields)
		getFieldOffset = 2
		versionIndex = versionFieldIndex
	} else {
		getFieldCount := len(t.getFields)
		queryFieldCount = getFieldCount + 1
		queryFields = make([]string, queryFieldCount)
		queryFieldReceivers = make([]interface{}, queryFieldCount)
		queryFieldConverters = make([]fieldConverter, queryFieldCount)

		queryFields[0] = versionFieldName
		queryFieldReceivers[0] = versionFieldMeta.getReceiver()
		queryFieldConverters[0] = versionFieldMeta.getConverter()
		versionIndex = 0

		getFieldOffset = 1
		for i := 0; i < getFieldCount; i++ {
			fieldMeta := t_meta.getFieldMeta(t.getFields[i])
			queryFields[i+getFieldOffset] = t.getFields[i]
			queryFieldReceivers[i+getFieldOffset] = fieldMeta.getReceiver()
			queryFieldConverters[i+getFieldOffset] = fieldMeta.getConverter()
		}
	}

	sb := strings.Builder{}

	sb.WriteString("select ")
	sb.WriteString(queryFields[0])
	for i := 1; i < queryFieldCount; i++ {
		sb.WriteString(",")
		sb.WriteString(queryFields[i])
	}

	sb.WriteString(fmt.Sprintf("where %s = '%s'", keyFieldName, t.key))

	if t.getWithVersion {
		sb.WriteString(fmt.Sprintf(" and %s %s %d;", versionFieldName, t.versionOp, t.version))
	} else {
		sb.WriteString(";")
	}

	row := db.QueryRowx(sb.String())

	if err := row.Scan(queryFieldReceivers...); err == sql.ErrNoRows {
		t.errCode = errcode.ERR_RECORD_NOTEXIST
	} else if err != nil {
		logger.Logger().Errorf("get table(%s) key(%s) failed: %s.", t.table, t.key, err)
		t.errCode = errcode.ERR_SQLERROR
	} else {
		t.errCode = errcode.ERR_OK
		t.version = queryFieldConverters[versionIndex](queryFieldReceivers[versionIndex]).(int64)

		for i := getFieldOffset; i < queryFieldCount; i++ {
			fieldName := queryFields[i]
			t.fields[fieldName] = proto.PackField(fieldName, queryFieldConverters[i](queryFieldReceivers[i]))
		}
	}
}

func get(conn *cliConn, msg *net.Message) {
	req := msg.GetData().(*proto.GetReq)

	head := msg.GetHead()

	table, key := head.SplitUniKey()

	tableMeta := getDBMeta().getTableMeta(table)

	if tableMeta == nil {
		logger.Errorf("get table(%s): table not exist.", table)
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_TABLE, &proto.GetResp{}))
		return
	}

	if b, i := tableMeta.checkFields(req.GetFields()); !b {
		logger.Errorf("get table(%s): invalid field(%s).", table, req.GetFields()[i])
		_ = conn.sendMessage(newMessage(head.Seqno, errcode.ERR_INVAILD_FIELD, &proto.GetResp{}))
		return
	}

	processDeadline, respDeadline := getDeadline(head.Timeout)

	cmd := &cmdGet{
		cmdBase: newCmdBase(conn, head.Seqno, head.UniKey, table, key, processDeadline, respDeadline),
		getAll:  req.GetAll(),
		fields:  req.GetFields(),
		version: req.Version,
	}

	pushCmd(cmd)
}
