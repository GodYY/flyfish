package kvnode

import (
	"fmt"
	//"encoding/binary"
	//"github.com/sniperHW/flyfish/conf"
	"github.com/sniperHW/flyfish/dbmeta"
	futil "github.com/sniperHW/flyfish/util"
	//"github.com/sniperHW/flyfish/proto"
	//"github.com/sniperHW/kendynet/timer"
	"github.com/sniperHW/kendynet/util"
	"go.etcd.io/etcd/etcdserver/api/snap"
	//"go.etcd.io/etcd/raft/raftpb"
	//"log"
	//	"math"
	//	"strings"
	"sync"
	"sync/atomic"
	//	"time"
	"github.com/sniperHW/flyfish/errcode"
	"unsafe"
)

var kvSlotSize int = 129

type kvSlot struct {
	sync.Mutex
	tmp      map[string]*kv
	elements map[string]*kv
	store    *kvstore
}

func (this *kvSlot) removeTmpKv(k *kv) {
	this.Lock()
	defer this.Unlock()
	delete(this.tmp, k.uniKey)
}

func (this *kvSlot) getRaftNode() *raftNode {
	return this.store.rn
}

func (this *kvSlot) getKvNode() *kvnode {
	return this.store.kvnode
}

//发起一致读请求
func (this *kvSlot) issueReadReq(task asynTaskI) {
	this.store.issueReadReq(task)
}

//发起更新请求
func (this *kvSlot) issueUpdate(task asynTaskI) {
	this.store.issueUpdate(task)
}

//请求向所有副本中新增kv
func (this *kvSlot) issueAddkv(task asynTaskI) {
	this.store.issueAddkv(task)
}

// a key-value store backed by raft
type kvstore struct {
	sync.Mutex
	proposeC       *util.BlockQueue
	readReqC       *util.BlockQueue
	snapshotter    *snap.Snapshotter
	slots          []*kvSlot
	kvcount        int //所有slot中len(elements)的总和
	kvKickingCount int //当前正在执行kicking的kv数量
	kvNode         *kvnode
	stop           func()
	rn             *raftNode
}

func (this *kvstore) getKvNode() *kvnode {
	return this.kvNode
}

func (this *kvstore) getSlot(uniKey string) *kvSlot {
	return this.slots[futil.StringHash(uniKey)%len(this.slots)]
}

//发起一致读请求
func (this *kvstore) issueReadReq(task asynTaskI) {
	if err := this.readReqC.AddNoWait(c); nil != err {
		task.onError(errcode.ERR_SERVER_STOPED)
	}
}

//发起更新请求
func (this *kvstore) issueUpdate(task asynTaskI) {
	if err := this.proposeC.AddNoWait(c); nil != err {
		task.onError(errcode.ERR_SERVER_STOPED)
	}
}

//请求向所有副本中新增kv
func (this *kvstore) issueAddkv(task asynTaskI) {
	if err := this.proposeC.AddNoWait(c); nil != err {
		task.onError(errcode.ERR_SERVER_STOPED)
	}
}

type storeMgr struct {
	sync.RWMutex
	stores map[int]*kvstore
	mask   int
	dbmeta *dbmeta.DBMeta
}

func (this *storeMgr) getkv(table string, key string) (*kv, error) {

	uniKey := makeUniKey(table, key)

	var k *kv = nil
	var err error

	store := this.getStore(uniKey)
	if nil != store {
		slot := store.getSlot(uniKey)
		slot.Lock()

		k, ok := slot.elements[uniKey]
		if !ok {
			k, ok = slot.tmp[uniKey]
		}

		if ok {
			if !this.dbmeta.CheckMetaVersion(k.meta.Version()) {
				newMeta := this.dbmeta.GetTableMeta(table)
				if newMeta != nil {
					atomic.StorePointer((*unsafe.Pointer)(unsafe.Pointer(&k.meta)), unsafe.Pointer(newMeta))
				} else {
					//log error
					err = fmt.Errorf("missing table meta")
				}
			}
		} else {

			meta := this.dbmeta.GetTableMeta(table)
			if meta == nil {
				err = fmt.Errorf("missing table meta")
			} else {
				k = newkv(slot, meta, key, uniKey, true)
				slot.tmp[uniKey] = k
			}
		}

		slot.Unlock()
	}

	return k, err
}

func (this *storeMgr) getStore(uniKey string) *kvstore {
	this.RLock()
	defer this.RUnlock()
	index := (futil.StringHash(uniKey) % this.mask) + 1
	return this.stores[index]
}

func (this *storeMgr) addStore(index int, store *kvstore) bool {
	if 0 == index || nil == store {
		panic("0 == index || nil == store")
	}
	this.Lock()
	defer this.Unlock()
	_, ok := this.stores[index]
	if ok {
		return false
	}
	this.stores[index] = store
	return true
}

func (this *storeMgr) stop() {
	this.RLock()
	defer this.RUnlock()
	for _, v := range this.stores {
		v.stop()
	}
}

func newStoreMgr(mutilRaft *mutilRaft) (*storeMgr, error) {

}
