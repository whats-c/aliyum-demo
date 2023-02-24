package worker

import (
	"container/list"
	"encoding/json"
	"fmt"

	"time"
)

type TableNode struct {
	ParentDir string
	TableName string

	Column map[string]string

	TimeStamp time.Time
}

type DataEntry struct {
	Id string

	Raw       []byte
	Structure interface{}

	Location *TableNode

	Timestamp time.Time
}

var global_table_list list.List
var global_data_list list.List

func DataEntry_Find(id string) *list.Element {
	for e := global_data_list.Front(); e != nil; e = e.Next() {
		if e.Value.(*DataEntry).Id == id {
			return e
		}
	}
	return nil
}

func DataEntry_Register(raw []byte, id string) (*DataEntry, bool) {
	var entry *DataEntry = new(DataEntry)

	if raw == nil {
		return nil, true
	}

	err := json.Unmarshal(raw, &entry.Structure)
	if err != nil {

		return nil, false
	}

	entry.Id = id
	entry.Location = nil
	entry.Raw = raw
	entry.Timestamp = time.Now()

	dataentry_ele := global_data_list.PushBack(entry)

	if dataentry_ele == nil {
		return nil, false
	}
	return entry, true
}

func (de *DataEntry) DataEntry_Unregister() bool {
	listele := de.DataEntry_Contianer()
	if listele == nil {
		return false
	}
	global_data_list.Remove(listele)
	return true
}

func (de *DataEntry) DataEntry_Contianer() *list.Element {
	for e := global_data_list.Front(); e != nil; e = e.Next() {
		if e.Value.(*DataEntry).Id == de.Id {
			return e
		}
	}
	return nil
}

func DataInsert(entry *DataEntry) bool {
	if entry == nil {
		fmt.Printf("The data is nil, please check!\n\r")
		return false
	}

	/* 	fmt.Printf("databasename:%s, tablename: %s\n\r", entry.Location.ParentDir, entry.Location.TableName)
	   	if entry.Structure == nil {
	   		return false
	   	} */
	map_data := entry.Structure.(map[string]interface{})
	for k, v := range map_data {
		switch vv := v.(type) {
		case string:
			fmt.Println(k, "is string", vv)
		case float64:
			fmt.Println(k, "is float64", vv)
		case []interface{}:
			fmt.Println(k, "is an array:")
			for i, u := range vv {
				fmt.Println(i, u)
			}
		default:
			fmt.Println(k, "is of a type I don't know how to handle")
		}
	}
	return true
}

func Column_generate(entry *DataEntry) map[string]string {

	var column map[string]string = make(map[string]string, 100)

	for k, v := range entry.Structure.(map[string]interface{}) {
		switch vv := v.(type) {
		case string:
			column[k] = "string"
		case float64:
			column[k] = "float64"
		case []interface{}:
			column[k] = "arry"
		default:
			_ = vv
		}
	}

	return column
}

func TableName_Find(tablename string) *list.Element {

	for e := global_data_list.Front(); e != nil; e = e.Next() {
		if e.Value.(*TableNode).TableName == tablename {
			return e
		}
	}
	return nil
}

func TableNode_Register(parentdir string, tablename string, column map[string]string) *TableNode {
	tablenode := new(TableNode)

	tablenode.ParentDir = parentdir
	tablenode.TableName = tablename
	tablenode.Column = column
	tablenode.TimeStamp = time.Now()

	return tablenode
}

func (tn *TableNode) TableName_Unregister() bool {
	listele := tn.TableName_Contianer()
	if listele == nil {
		return false
	}
	global_table_list.Remove(listele)
	return true
}

func (tn *TableNode) TableName_Contianer() *list.Element {
	for e := global_table_list.Front(); e != nil; e = e.Next() {
		if e.Value.(*TableNode).TableName == tn.TableName {
			return e
		}
	}
	return nil
}

func Init() {
	// initialize the global table list
	global_table_list.Init()

	// initialize the global data list
	global_data_list.Init()

}
