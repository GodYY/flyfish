package node_sql

import (
	"fmt"
	"github.com/jmoiron/sqlx"
	"github.com/sniperHW/flyfish/errcode"
	"github.com/sniperHW/flyfish/net"
	"github.com/sniperHW/flyfish/proto"
	"strconv"
	"strings"
	"sync/atomic"
	"teacher/node/common/logger"
)

type fieldConverter func(interface{}) interface{}

var (
	fieldTypeName2Type = map[string]proto.ValueType{
		"int":    proto.ValueType_int,
		"float":  proto.ValueType_float,
		"string": proto.ValueType_string,
		"blob":   proto.ValueType_blob,
	}

	fieldType2Getter = map[proto.ValueType]func() interface{}{
		proto.ValueType_int:    func() interface{} { return new(int64) },
		proto.ValueType_float:  func() interface{} { return new(float64) },
		proto.ValueType_string: func() interface{} { return new(string) },
		proto.ValueType_blob:   func() interface{} { return new([]byte) },
	}

	fieldType2Converter = map[proto.ValueType]fieldConverter{
		proto.ValueType_int:    func(v interface{}) interface{} { return *v.(*int64) },
		proto.ValueType_float:  func(v interface{}) interface{} { return *v.(*float64) },
		proto.ValueType_string: func(v interface{}) interface{} { return *v.(*string) },
		proto.ValueType_blob:   func(v interface{}) interface{} { return *v.(*[]byte) },
	}

	fieldType2GetDefaultV = map[proto.ValueType]func(string) interface{}{
		proto.ValueType_int: func(s string) interface{} {
			if s == "" {
				return int64(0)
			} else if v, err := strconv.ParseInt(s, 10, 64); err == nil {
				return v
			} else {
				return nil
			}
		},

		proto.ValueType_float: func(s string) interface{} {
			if s == "" {
				return float64(0)
			} else if v, err := strconv.ParseInt(s, 10, 64); err == nil {
				return v
			} else {
				return nil
			}
		},

		proto.ValueType_string: func(s string) interface{} {
			return s
		},

		proto.ValueType_blob: func(s string) interface{} {
			return []byte{}
		},
	}
)

func getFieldTypeByStr(str string) proto.ValueType {
	if t, ok := fieldTypeName2Type[str]; ok {
		return t
	} else {
		return proto.ValueType_invaild
	}
}

func getFieldGetterByType(t proto.ValueType) func() interface{} {
	return fieldType2Getter[t]
}

func getFieldConverterByType(t proto.ValueType) func(interface{}) interface{} {
	if f, ok := fieldType2Converter[t]; ok {
		return f
	} else {
		return nil
	}
}

func getFieldDefaultValue(t proto.ValueType, str string) interface{} {
	if f, ok := fieldType2GetDefaultV[t]; ok {
		return f(str)
	} else {
		return nil
	}
}

func isValidFieldType(t proto.ValueType) bool {
	return t != proto.ValueType_nil && t != proto.ValueType_invaild
}

type fieldMeta struct {
	name     string          // 字段名
	typ      proto.ValueType // 字段值类型
	defaultV interface{}     // 字段默认值
}

func (f *fieldMeta) getReceiver() interface{} {
	return getFieldGetterByType(f.typ)()
}

func (f *fieldMeta) getConverter() func(interface{}) interface{} {
	return getFieldConverterByType(f.typ)
}

const (
	keyFieldName      = "__key__"
	keyFieldIndex     = 0
	versionFieldName  = "__version__"
	versionFieldIndex = 1
)

var (
	keyFieldMeta = &fieldMeta{
		name:     keyFieldName,
		typ:      proto.ValueType_string,
		defaultV: getFieldDefaultValue(proto.ValueType_string, ""),
	}

	versionFieldMeta = &fieldMeta{
		name:     versionFieldName,
		typ:      proto.ValueType_int,
		defaultV: getFieldDefaultValue(proto.ValueType_int, ""),
	}
)

type tableMeta struct {
	name               string                // 表名
	fieldMetas         map[string]*fieldMeta // 字段meta
	allFieldNames      []string              // 包括'__key__'和'__version__'字段
	allFieldGetters    []func() interface{}  //
	allFieldConverters []fieldConverter      //
}

func (t *tableMeta) getFieldMeta(field string) *fieldMeta {
	return t.fieldMetas[field]
}

func (t *tableMeta) getFieldCount() int {
	return len(t.fieldMetas)
}

func (t *tableMeta) checkFields(fields []string) (bool, int) {
	for i, v := range fields {
		if t.fieldMetas[v] == nil {
			return false, i
		}
	}

	return true, 0
}

func (t *tableMeta) getAllFields() []string {
	return t.allFieldNames
}

func (t *tableMeta) getAllFieldReceivers() []interface{} {
	//if len(t.allFieldNewer) != len(t.allFieldNames) {
	//	panic("impossible")
	//}

	receivers := make([]interface{}, len(t.allFieldNames))
	for i, f := range t.allFieldGetters {
		receivers[i] = f()
	}

	return receivers
}

func (t *tableMeta) getAllFieldConverter() []fieldConverter {
	return t.allFieldConverters
}

type dbMeta struct {
	tableMetas  atomic.Value
	keyMeta     *fieldMeta
	versionMeta *fieldMeta
	version     int64
}

func (d *dbMeta) setTableMetas(tableMetas map[string]*tableMeta) {
	d.tableMetas.Store(tableMetas)
}

func (d *dbMeta) getTableMeta(table string) *tableMeta {
	tableMetas := d.tableMetas.Load().(map[string]*tableMeta)
	return tableMetas[table]
}

type tableDef struct {
	name     string
	fieldDef string
}

var (
	db_meta *dbMeta
)

func initDBMeta() error {
	tableDef, err := loadTableDef()
	if err != nil {
		return err
	}

	tableMetas, err := createTableMetasByTableDef(tableDef)
	if err != nil {
		return err
	}

	db_meta = &dbMeta{
		version: 0,
	}
	db_meta.tableMetas.Store(tableMetas)

	return nil
}

func createTableMetasByTableDef(def []*tableDef) (map[string]*tableMeta, error) {
	tableMetas := make(map[string]*tableMeta, len(def))
	for _, t := range def {
		fieldDefStr := strings.Split(t.fieldDef, ",")

		fieldCount := len(fieldDefStr)
		if fieldCount == 0 {
			return nil, fmt.Errorf("%s: no field", t.name)
		}

		allFieldCount := fieldCount + 2
		tMeta := &tableMeta{
			name:               t.name,
			fieldMetas:         make(map[string]*fieldMeta, len(fieldDefStr)),
			allFieldNames:      make([]string, allFieldCount),
			allFieldGetters:    make([]func() interface{}, allFieldCount),
			allFieldConverters: make([]fieldConverter, allFieldCount),
		}

		tMeta.allFieldNames[keyFieldIndex] = keyFieldMeta.name
		tMeta.allFieldGetters[keyFieldIndex] = getFieldGetterByType(keyFieldMeta.typ)
		tMeta.allFieldConverters[keyFieldIndex] = getFieldConverterByType(keyFieldMeta.typ)

		tMeta.allFieldNames[versionFieldIndex] = versionFieldMeta.name
		tMeta.allFieldGetters[versionFieldIndex] = getFieldGetterByType(versionFieldMeta.typ)
		tMeta.allFieldConverters[versionFieldIndex] = getFieldConverterByType(versionFieldMeta.typ)

		var (
			allFieldIndex = 2
			fieldName     string
			fieldType     proto.ValueType
			fieldDefaultV interface{}
		)

		for i, v := range fieldDefStr {
			s := strings.Split(v, ":")

			if len(s) != 3 {
				return nil, fmt.Errorf("%s: field %dth '%s': invalid", t.name, i, v)
			}

			fieldName = s[0]

			if fieldName == "" || fieldName == keyFieldMeta.name || fieldName == versionFieldMeta.name {
				return nil, fmt.Errorf("%s: field %dth '%s': name invalid", t.name, i, v)
			}

			if _, ok := tMeta.fieldMetas[s[0]]; ok {
				return nil, fmt.Errorf("%s: field %dth '%s': name repeated", t.name, i, v)
			}

			if fieldType = getFieldTypeByStr(s[1]); !isValidFieldType(fieldType) {
				return nil, fmt.Errorf("%s: field %dth '%s': type invalid", t.name, i, v)
			}

			if fieldDefaultV = getFieldDefaultValue(fieldType, s[2]); fieldDefaultV == nil {
				return nil, fmt.Errorf("%s: field %dth '%s': default-value invalid", t.name, i, v)
			}

			tMeta.fieldMetas[fieldName] = &fieldMeta{
				name:     fieldName,
				typ:      fieldType,
				defaultV: fieldDefaultV,
			}

			tMeta.allFieldNames[allFieldIndex] = fieldName
			tMeta.allFieldGetters[allFieldIndex] = getFieldGetterByType(fieldType)
			tMeta.allFieldConverters[allFieldIndex] = getFieldConverterByType(fieldType)
			allFieldIndex++
		}

		tableMetas[tMeta.name] = tMeta
	}

	return tableMetas, nil
}

func loadTableDef() ([]*tableDef, error) {
	var db *sqlx.DB
	var err error
	dbConfig := getNodeConfig().DBConfig

	db, err = dbOpen(dbConfig.SqlType, dbConfig.ConfDbHost, dbConfig.ConfDbPort, dbConfig.ConfDataBase, dbConfig.ConfDbUser, dbConfig.ConfDbPassword)

	if nil != err {
		return nil, err
	} else {

		if rows, err := db.Queryx("select __table__,__conf__ from table_conf;"); err != nil {
			return nil, err
		} else {
			var defs []*tableDef

			for rows.Next() {
				def := new(tableDef)
				if err = rows.Scan(&def.name, &def.fieldDef); err != nil {
					break
				}

				defs = append(defs, def)
			}

			_ = rows.Close()

			_ = db.Close()

			return defs, err
		}
	}
}

func reloadTableMeta(cli *cliConn, msg *net.Message) {
	head := msg.GetHead()

	var (
		tableDef   []*tableDef
		tableMetas map[string]*tableMeta
		err        error
		resp       = &proto.ReloadTableConfResp{}
	)

	if tableDef, err = loadTableDef(); err != nil {
		resp.Err = fmt.Sprintf("load table-def failed: %s", err)
		logger.Logger().Errorln(resp.Err)
		head.ErrCode = errcode.ERR_OTHER
	} else if tableMetas, err = createTableMetasByTableDef(tableDef); err != nil {
		resp.Err = fmt.Sprintf("create table-meta failed: %s", err)
		logger.Logger().Errorln(resp.Err)
		head.ErrCode = errcode.ERR_OTHER
	} else {
		db_meta.setTableMetas(tableMetas)
		head.ErrCode = errcode.ERR_OK
	}

	_ = cli.sendMessage(net.NewMessage(head, resp))
}

func getDBMeta() *dbMeta {
	return db_meta
}
