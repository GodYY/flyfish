package errcode

const (
	ERR_OK = int32(iota)
	ERR_RETRY
	ERR_BUSY
	ERR_VERSION_MISMATCH
	ERR_RECORD_EXIST //key已经存在
	ERR_TIMEOUT
	ERR_SERVER_STOPED
	ERR_SQLERROR
	ERR_NOT_LEADER
	ERR_RAFT
	ERR_SEND_FAILED
	ERR_RECORD_NOTEXIST
	ERR_MISSING_FIELDS //缺少字段
	ERR_MISSING_TABLE  //没有指定表
	ERR_MISSING_KEY    //没有指定key
	ERR_INVAILD_TABLE  //非法表
	ERR_INVAILD_FIELD  //非法字段
	ERR_CAS_NOT_EQUAL
	ERR_PROPOSAL_DROPPED
	ERR_CONNECTION
	ERR_OTHER
	ERR_RECORD_UNCHANGE
	ERR_END
)

var err_str []string = []string{
	"OK",
	"RETRY",
	"BUSY",
	"VERSION_MISMATCH",
	"RECORD_EXIST",
	"TIMEOUT",
	"SERVER_STOPED",
	"SQLERROR",
	"NOT_LEADER",
	"RAFT",
	"SEND_FAILED",
	"RECORD_NOTEXIST",
	"MISSING_FIELDS",
	"MISSING_TABLE",
	"MISSING_KEY",
	"INVAILD_TABLE",
	"INVAILD_FIELD",
	"CAS_NOT_EQUAL",
	"PROPOSAL_DROPPED",
	"CONNECTION",
	"OTHER",
	"RECORD_UNCHANGE",
}

func GetErrorStr(code int32) string {

	if code >= 0 && code < ERR_END {
		return err_str[code]
	} else {
		return "invaild errcode"
	}
}
