package worker

import (
	"fmt"
	"testing"
)

func TestDataEntry(t *testing.T) {
	buf := []byte(`{"one":"one","two":"two" }`)

	dataentry, ok := DataEntry_Register(buf, "test001")
	if !ok {
		fmt.Printf("dataentry register return a error!\n\r")
	}

	ok = DataInsert(dataentry)
	if !ok {
		fmt.Printf("data insert return a error!\n\r")
	}

	ele001 := DataEntry_Find("test001")
	ele002 := dataentry.DataEntry_Contianer()
	if ele001 != ele002 {
		fmt.Printf("ele001 not equal ele002!\n\r")
	}

}

func TestTableNode(t *testing.T) {
	column := map[string]string{
		"one": "string",
		"two": "int",
	}
	tablename := TableNode_Register("defaultdatabase", "tabletest001", column)
	if tablename == nil {
		fmt.Printf("tablenode register return a error!\n\r")
	}
	fmt.Printf("tablename: parentdir-%s, tablename-%s\n\r", tablename.ParentDir, tablename.TableName)
	for k, v := range tablename.Column {
		fmt.Printf("key:%s, value:%s\n\r", k, v)
	}

	ele001 := TableName_Find("tabletest001")
	ele002 := tablename.TableName_Contianer()
	if ele001 != ele002 {
		fmt.Printf("ele001 not equal ele002\n\r")
	}
}
