package flyfish

import (
	codec "flyfish/codec"
	"flyfish/conf"
	"flyfish/proto"
	pb "github.com/golang/protobuf/proto"
	"github.com/sniperHW/kendynet"
	"io/ioutil"
)

func reloadConf(session kendynet.StreamSession, msg *codec.Message) {
	oldConf := conf.GetConfig()
	path := msg.GetData().(*proto.ReloadConfigReq).GetPath()
	err := conf.LoadConfig(path)
	if nil == err {

		bs, _ := ioutil.ReadFile(path)

		session.Send(&proto.ReloadConfigResp{
			Err: pb.String("-------------reload ok---------------\n" + string(bs) + "\n-------------reload ok---------------\n"),
		})
		newConf := conf.GetConfig()

		if oldConf.SqlUpdateQueueSize != newConf.SqlUpdateQueueSize {
			updateSqlUpdateQueueSize(newConf.SqlUpdateQueueSize)
		}

		if oldConf.SqlLoadQueueSize != newConf.SqlLoadQueueSize {
			updateSqlLoadQueueSize(newConf.SqlLoadQueueSize)
		}

		if oldConf.RedisQueueSize != newConf.RedisQueueSize {
			updateRedisQueueSize(newConf.RedisQueueSize)
		}

		UpdateLogConfig()

	} else {
		session.Send(&proto.ReloadConfigResp{
			Err: pb.String(err.Error()),
		})
	}
}