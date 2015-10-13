package sqlproxy

import (
	"fmt"
	"testing"
)

func connect() (*SqlProxy, error) {
	db := NewSqlProxy("root", "1881982050~!@", "123.59.24.181", "3306", "game")
	err := db.Connect()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func TestQuery(t *testing.T) {
	db, err := connect()
	if err != nil {
		t.Log(err)
		t.Fail()
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
	}

	for _, dataMap := range resultMap {
		for key, value := range dataMap {
			fmt.Printf("key:%s value:%s\n", key, value)
		}
	}
}

func TestUpdate(t *testing.T) {
	db, err := connect()
	if err != nil {
		t.Log(err)
		t.Fail()
	}

	feilds := make([]*FeildData, 0, 32)
	feilds = append(feilds, &FeildData{Name: "last_update_time", Value: "2015-12-05 10:00:00"})

	saveCmd := &SaveCmd{
		TableName: "users",
		IsNew:     false,
		Feilds:    feilds[:],
		Condition: &FeildData{Name: "user_name", Value: "rex"},
	}

	db.SaveData(saveCmd)
}
