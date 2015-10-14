package sqlproxy

import (
	"database/sql"
	"errors"
	_ "github.com/go-sql-driver/mysql"
	"log"
	"time"
)

const (
	saveCmdMaxLen = 128
)

type FeildData struct {
	Name  string
	Value string
}

type QueryCmd struct {
	TableName  string
	FieldNames []string
}

type SaveCmd struct {
	TableName string
	Feilds    []*FeildData
	Condition *FeildData
	IsNew     bool
}

type SqlProxy struct {
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
			this.saveData(cmd)
		case <-this.quitEvent:
			goto QUIT
		case <-time.After(time.Second):
		}
	}

QUIT:
}

func (this *SqlProxy) saveData(cmd *SaveCmd) error {
	if this.db == nil {
		return errors.New("connection already disconnect")
	}

	var sqlStr string

	if cmd.IsNew {
		sqlStr = "insert into " + cmd.TableName
		feildNamesStr := "("
		valuesStr := "("

		for i, feildData := range cmd.Feilds {
			if i == 0 {
				feildNamesStr = feildNamesStr + ""
				valuesStr = valuesStr + ""
			} else {
				feildNamesStr = feildNamesStr + ","
				valuesStr = valuesStr + ","
			}

			feildNamesStr = feildNamesStr + feildData.Name
			valuesStr = valuesStr + "'" + feildData.Value + "'"

		}

		feildNamesStr = feildNamesStr + ")"
		valuesStr = valuesStr + ")"

		sqlStr = sqlStr + feildNamesStr + " values " + valuesStr
	} else {
		sqlStr = "update " + cmd.TableName + " set"
		for i, feildData := range cmd.Feilds {
			if i == 0 {
				sqlStr = sqlStr + " "
			} else {
				sqlStr = sqlStr + ","
			}

			sqlStr = sqlStr + feildData.Name + " = '" + feildData.Value + "'"
		}

		condition := cmd.Condition
		if condition.Name != "" {
			sqlStr = sqlStr + " where " + condition.Name + " = '" + condition.Value + "'"
		}
	}

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

func (this *SqlProxy) LoadData(queryData *QueryCmd) ([]map[string]string, error) {
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
	log.Println("query string:", queryString)

	rows, err := this.db.Query(queryString)
	if err != nil {
		return nil, err
	}

	defer rows.Close()

	dataMapList := make([]map[string]string, 0, 32)
	for rows.Next() {
		fieldNames := queryData.FieldNames
		dataMap := make(map[string]string)
		fieldLen := len(fieldNames)
		results := make([]string, fieldLen)
		interfaces := make([]interface{}, fieldLen)

		for i := 0; i < fieldLen; i++ {
			interfaces[i] = &results[i]
		}

		rows.Scan(interfaces...)

		for i, fieldName := range fieldNames {
			dataMap[fieldName] = results[i]
		}

		dataMapList = append(dataMapList, dataMap)
	}

	return dataMapList, nil
}
