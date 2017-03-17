package sqlproxy

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"log"
    "time"
	"sync"
)

const (
	saveCmdMaxLen = 8192
)

type FieldData struct {
	Name  string
	Value string
}

type QueryCmd struct {
	TableName  string
	FieldNames []string
	Condition  *FieldData
}

type SaveCmd struct {
	TableName string
	Fields    []*FieldData
	Condition *FieldData
	IsNew     bool
}

type SqlProxy struct {
	sync.RWMutex
	user        string
	password    string
	ip          string
	port        string
	dbName      string
	db          *sql.DB
	saveCmdList chan *SaveCmd
	quitEvent   chan int
}

func (this *SqlProxy) messageLoop() {
	for {
		select {
		case cmd := <-this.saveCmdList:
RETRY:
			err := this.SaveData(cmd)
            if err != nil {
                log.Println("[-]", err)
                if len(this.saveCmdList) < saveCmdMaxLen {
                    time.Sleep(time.Second * 10)
                    select {
                    case <-this.quitEvent:
                        return
                    default:
                        goto RETRY
                    }
                }
            }
		case <-this.quitEvent:
			return
		}
	}
}

func (this *SqlProxy) SaveData(cmd *SaveCmd) error {
	this.Lock()
	defer this.Unlock()

	if this.db == nil {
		return errors.New("connection already disconnect")
	}

	var sqlStr string

	if cmd.IsNew {
		sqlStr = "insert into " + cmd.TableName
		fieldNamesStr := "("
		valuesStr := "("

		for i, fieldData := range cmd.Fields {
			if i == 0 {
				fieldNamesStr = fieldNamesStr + ""
				valuesStr = valuesStr + ""
			} else {
				fieldNamesStr = fieldNamesStr + ","
				valuesStr = valuesStr + ","
			}

			fieldNamesStr = fieldNamesStr + fieldData.Name
			valuesStr = valuesStr + "'" + fieldData.Value + "'"

		}

		fieldNamesStr = fieldNamesStr + ")"
		valuesStr = valuesStr + ")"

		sqlStr = sqlStr + fieldNamesStr + " values " + valuesStr
	} else {
		sqlStr = "update " + cmd.TableName + " set"
		for i, fieldData := range cmd.Fields {
			if i == 0 {
				sqlStr = sqlStr + " "
			} else {
				sqlStr = sqlStr + ","
			}

			sqlStr = sqlStr + fieldData.Name + " = '" + fieldData.Value + "'"
		}

		condition := cmd.Condition
		if condition != nil && condition.Name != "" {
			sqlStr = sqlStr + " where " + condition.Name + " = '" + condition.Value + "'"
		}
	}

	log.Println(sqlStr)
	_, err := this.db.Exec(sqlStr)
	if err != nil {
		return err
	}

	return nil
}

func NewSqlProxy(user string, password string, ip string, port string, dbName string) *SqlProxy {
	sqlProxy := &SqlProxy{
		user:     user,
		password: password,
		ip:       ip,
		port:     port,
		dbName:   dbName,
	}

	go sqlProxy.messageLoop()

	return sqlProxy
}

func (this *SqlProxy) GetSaveCmdList() chan<- *SaveCmd {
	return this.saveCmdList
}

func (this *SqlProxy) PushSaveCmd(saveCmd *SaveCmd) {
	this.saveCmdList <- saveCmd
}

func (this *SqlProxy) Connect() error {
	if this.db != nil {
		return errors.New("connection already connect")
	}

	connStr := this.user + ":" + this.password + "@tcp(" + this.ip + ":" + this.port + ")/" + this.dbName + "?charset=utf8"

	db, err := sql.Open("mysql", connStr)
	if err != nil {
		return err
	}

	this.db = db
	this.saveCmdList = make(chan *SaveCmd, saveCmdMaxLen)
	this.quitEvent = make(chan int, 1)
	return nil
}

func (this *SqlProxy) Close() error {
	if this.db == nil {
		return errors.New("connection already disconnect")
	}

	this.db.Close()
	this.db = nil

	this.quitEvent <- 1

	return nil
}

func (this* SqlProxy) GetTop(fieldName string, table string) uint {
	this.RLock()
	defer this.RUnlock()
    queryString := "selete " + fieldName + " from " + table + 
                   " order by " + fieldName  + " desc limit 1"

	rows, err := this.db.Query(queryString)
	if err != nil {
		return 0
	}

    rows.Next()
    maxUid := uint(0)
    err = rows.Scan(&maxUid)
    if err != nil {
        return 0
    }

    return maxUid
}

func (this *SqlProxy) LoadData(queryData *QueryCmd) ([]map[string]string, error) {
	this.RLock()
	defer this.RUnlock()

	if this.db == nil {
		return nil, errors.New("connection already disconnect")
	}

	queryString := "select"
	for i, fieldName := range queryData.FieldNames {
		var delmiter string
		if i != 0 {
			delmiter = ", "
		} else {
			delmiter = " "
		}
		queryString = queryString + delmiter + fieldName
	}

	queryString = queryString + " from " + queryData.TableName
	condition := queryData.Condition
	if condition != nil && condition.Name != "" {
		queryString = queryString + " where " + condition.Name + " = '" + condition.Value + "'"
	}

	log.Println("[?] query string:", queryString)
	rows, err := this.db.Query(queryString)
	if err != nil {
		return nil, err
	}
	log.Println("[?] after query string")

	dataMapList := make([]map[string]string, 0, 4096)

	for rows.Next() {
		fieldNames := queryData.FieldNames
		dataMap := make(map[string]string)
		fieldLen := len(fieldNames)
		results := make([]string, fieldLen)
		interfaces := make([]interface{}, fieldLen)

		for i := 0; i < fieldLen; i++ {
			interfaces[i] = &results[i]
		}

		err = rows.Scan(interfaces...)
		if err != nil {
			return dataMapList, err
		}

		for i, fieldName := range fieldNames {
			dataMap[fieldName] = results[i]
		}

		dataMapList = append(dataMapList, dataMap)
	}

	return dataMapList, nil
}
