package sqlproxy

import (
	_ "fmt"
	"testing"
    "log"
	"time"
)

func connect() (*SqlProxy, error) {
	db := NewSqlProxy("root", "Rex137955", "127.0.0.1", "3306", "game")
	err := db.Connect()
	if err != nil {
		return nil, err
	}

	return db, nil
}
/*
func TestQuery(t *testing.T) {
	db, err := connect()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	fieldArray := make([]string, 0, 32)
	fieldArray = append(fieldArray, "user_name")
	fieldArray = append(fieldArray, "last_update_time")

	queryData := &QueryCmd{
		TableName:  "users",
		FieldNames: fieldArray[:],
	}

	resultMap, err := db.LoadData(queryData)
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	for _, dataMap := range resultMap {
		for key, value := range dataMap {
			fmt.Printf("key:%s value:%s\n", key, value)
		}
	}
}

func TestDisconnect(t *testing.T) {
	db, err := connect()
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	db.Close()

	time.Sleep(1 * time.Second)
}
*/
func TestUpdate(t *testing.T) {
	db, err := connect()
	if err != nil {
		t.Log(err)
		t.Fail()
		return
	}

	fields := make([]*FieldData, 0, 32)
	fields = append(fields, &FieldData{Name: "last_update_time", Value: "2015-12-05 10:00:00"})

	saveCmd := &SaveCmd{
		TableName: "users",
		IsNew:     false,
		Fields:    fields[:],
		Condition: &FieldData{Name: "user_name", Value: "rex"},
	}

	list := db.GetSaveCmdList()
    log.Println("[*] ready")
	time.Sleep(10 * time.Second)
	list <- saveCmd
    log.Println("[*] end")

	time.Sleep(100 * time.Second)
}
