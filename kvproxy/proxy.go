package kvproxy

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	protocol "github.com/sniperHW/flyfish/proto"
	"github.com/sniperHW/kendynet"
	"github.com/sniperHW/kendynet/socket/listener/tcp"
	"github.com/sniperHW/kendynet/timer"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

type pendingReq struct {
	seqno         int64
	oriSeqno      int64
	session       kendynet.StreamSession
	deadlineTimer *timer.Timer
	processor     *reqProcessor
}

type kvproxy struct {
	router     *reqRouter
	processors []*reqProcessor
	listener   *tcp.Listener
	seqno      int64
}

func (this *pendingReq) onTimeout(_ *timer.Timer) {
	this.processor.Lock()
	defer this.processor.Unlock()
	delete(this.processor.pendingReqs, this.seqno)
}

type reqProcessor struct {
	sync.Mutex
	pendingReqs map[int64]*pendingReq
	timerMgr    *timer.TimerMgr
	router      *reqRouter
}

func newReqProcessor(router *reqRouter) *reqProcessor {
	return &reqProcessor{
		pendingReqs: map[int64]*pendingReq{},
		timerMgr:    timer.NewTimerMgr(),
		router:      router,
	}
}

func (this *reqProcessor) onReq(seqno int64, session kendynet.StreamSession, req *kendynet.ByteBuffer) {
	var err error
	var oriSeqno int64
	var lenUnikey int16
	var unikey string
	var timeout uint32

	if oriSeqno, err = req.GetInt64(5); nil != err {
		return
	}

	if lenUnikey, err = req.GetInt16(21); nil != err {
		return
	}

	if 0 == lenUnikey {
		return
	}

	if unikey, err = req.GetString(23, uint64(lenUnikey)); nil != err {
		return
	}

	if timeout, err = req.GetUint32(19); nil != err {
		return
	}

	//用seqno替换oriSeqno
	req.PutInt64(5, seqno)

	err = func() error {
		this.Lock()
		defer this.Unlock()
		err := this.router.forward2kvnode(unikey, time.Now().Add(time.Duration(timeout/2)*time.Millisecond), req, session.GetUserData().(bool))
		if nil == err {
			pReq := &pendingReq{
				seqno:     seqno,
				oriSeqno:  oriSeqno,
				session:   session,
				processor: this,
			}
			pReq.deadlineTimer = this.timerMgr.Once(time.Duration(timeout)*time.Millisecond, nil, pReq.onTimeout)
			this.pendingReqs[seqno] = pReq
		}
		return err
	}()

	if nil != err {
		//返回错误响应
	}
}

func (this *reqProcessor) onResp(seqno int64, resp *kendynet.ByteBuffer) {
	this.Lock()
	defer this.Unlock()
	req, ok := this.pendingReqs[seqno]
	if ok {
		//先删除定时器
		if req.deadlineTimer.Cancel() {
			delete(this.pendingReqs, seqno)
			//用oriSeqno替换seqno
			resp.PutInt64(5, req.oriSeqno)
			req.session.SendMessage(resp)
		}
	}
}

func sendLoginResp(session kendynet.StreamSession, loginResp *protocol.LoginResp) bool {
	conn := session.GetUnderConn().(*net.TCPConn)
	buffer := kendynet.NewByteBuffer(64)
	data, _ := proto.Marshal(loginResp)
	buffer.AppendUint16(uint16(len(data)))
	buffer.AppendBytes(data)

	conn.SetWriteDeadline(time.Now().Add(time.Second * 5))
	_, err := conn.Write(buffer.Bytes())
	conn.SetWriteDeadline(time.Time{})
	return nil == err
}

func recvLoginReq(session kendynet.StreamSession) (*protocol.LoginReq, error) {
	conn := session.GetUnderConn().(*net.TCPConn)
	buffer := make([]byte, 1024)
	w := 0
	pbsize := 0
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second * 5))
		n, err := conn.Read(buffer[w:])
		conn.SetReadDeadline(time.Time{})

		if nil != err {
			return nil, err
		}

		w = w + n

		if w >= 2 {
			pbsize = int(binary.BigEndian.Uint16(buffer[:2]))
		}

		if w >= pbsize+2 {
			loginReq := &protocol.LoginReq{}
			if err = proto.Unmarshal(buffer[2:w], loginReq); err != nil {
				return loginReq, nil
			} else {
				return nil, err
			}
		}
	}
}

func verifyLogin(loginReq *protocol.LoginReq) bool {
	return true
}

func NewKVProxy(addr string, processorCount int, kvnodes []string) *kvproxy {

	if processorCount == 0 || len(kvnodes) == 0 {
		return nil
	}

	var err error
	proxy := &kvproxy{}

	if proxy.listener, err = tcp.New("tcp", addr); nil != err {
		return nil
	}

	proxy.router = newReqRounter(kvnodes)
	proxy.processors = []*reqProcessor{}
	for i := 0; i < processorCount; i++ {
		proxy.processors = append(proxy.processors, newReqProcessor(proxy.router))
	}

	return proxy
}

func (this *kvproxy) onResp(resp *kendynet.ByteBuffer) {
	if seqno, err := resp.GetInt64(5); nil == err {
		processor := this.processors[seqno%int64(len(this.processors))]
		processor.onResp(seqno, resp)
	}
}

func (this *kvproxy) Start() error {
	if nil == this.listener {
		return fmt.Errorf("invaild listener")
	}

	return this.listener.Serve(func(session kendynet.StreamSession) {
		go func() {
			loginReq, err := recvLoginReq(session)
			if nil != err {
				session.Close("login failed", 0)
				return
			}

			if !verifyLogin(loginReq) {
				session.Close("login failed", 0)
				return
			}

			loginResp := &protocol.LoginResp{
				Ok:       true,
				Compress: loginReq.GetCompress(),
			}

			if !sendLoginResp(session, loginResp) {
				session.Close("login failed", 0)
				return
			}

			session.SetUserData(loginReq.GetCompress())

			session.SetReceiver(NewReceiver())

			session.Start(func(event *kendynet.Event) {
				if event.EventType == kendynet.EventTypeError {
					event.Session.Close(event.Data.(error).Error(), 0)
				} else {
					seqno := atomic.AddInt64(&this.seqno, 1)
					processor := this.processors[seqno%int64(len(this.processors))]
					processor.onReq(seqno, session, event.Data.(*kendynet.ByteBuffer))
				}
			})
		}()
	})
}
