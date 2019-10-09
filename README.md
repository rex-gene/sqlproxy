# sqlproxy



# create proxy
proxy := sqlproxy.NewSqlProxy("root", "123456", "123.59.11.11", "3306", "game")

# query datebase
fieldArray := make([]string, 0, 32)
fieldArray = append(fieldArray, "user_name")
fieldArray = append(fieldArray, "last_update_time")

queryData := &sqlproxy.QueryCmd{
  TableName:  "users",
  FieldNames: fieldArray[:],
}

resultMap, err := proxy.LoadData(queryData)
if err != nil {
}

for _, dataMap := range resultMap {
  for key, value := range dataMap {
    fmt.Printf("key:%s value:%s\n", key, value)
  }
}
