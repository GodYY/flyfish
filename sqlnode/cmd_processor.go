package node_sql

import (
	"github.com/jmoiron/sqlx"
	util2 "github.com/sniperHW/flyfish/util"
	"github.com/sniperHW/kendynet/util"
	"reflect"
	"teacher/node/common/logger"
)

type cmdProcessor struct {
	db       *sqlx.DB
	cmdQueue *util.BlockQueue
}

func newCmdProcessor(db *sqlx.DB) *cmdProcessor {
	return &cmdProcessor{
		db:       db,
		cmdQueue: util.NewBlockQueue(),
	}
}

func (p *cmdProcessor) start() {
	go p.process()
}

func (p *cmdProcessor) pushCmd(c cmd) {
	p.cmdQueue.AddNoWait(c)
}

func (p *cmdProcessor) process() {
	for {
		closed, list := p.cmdQueue.Get()

		var n = len(list)
		var task sqlTask
		var cb_next = false
		var i = 0

		for i < n {
			switch c := list[i].(type) {
			case *cmdGet:
				if c.isCancel() || c.isTimeout() {
					// todo something else ?
					break
				}

				if task == nil {
					task = c.makeSqlTask()
					cb_next = true
				} else {
					cb_next = task.combine(c)
				}

				if cb_next && i < n-1 {
					break
				}

				task.do(p.db)
				task.reply()
				task = nil

			default:
				logger.Logger().Errorf("invalid cmd type: %s.", reflect.TypeOf(list[i]))
			}

			i++
		}

		if closed {
			break
		}
	}
}

var (
	cmdProcessors []*cmdProcessor
)

func initCmdProcessor() {
	n := getNodeConfig().DBConnections

	cmdProcessors = make([]*cmdProcessor, n)
	for i := 0; i < n; i++ {
		cmdProcessors[i] = newCmdProcessor(getGlobalDB())
		cmdProcessors[i].start()
	}
}

func pushCmd(c cmd) {
	cmdProcessors[util2.StringHash(c.uniKey())%len(cmdProcessors)].pushCmd(c)
}
