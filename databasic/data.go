package databasic

import (
	"context"
	"time"
)

type DataClass struct {
	List *ListNode /* it is a continer that is used to orgnize the parent type as a list */

	Id string

	Node_list *ListNode /* it is a list holding the datanode, which is processed by some method */
	Node_num  int       /* the value records the numeber of Node_list holding. */
	Node_max  int       /* the memeber is unused */
}

func DataClass_register(id string) *DataClass {
	if id == "" || global_dataclass_num == MAX_DATACLASS_NUMBER {
		return nil
	}
	dataclass := new(DataClass)

	dataclass.Id = id
	dataclass.List = ListNode_create(dataclass)
	dataclass.Node_list = ListNode_create(dataclass)
	dataclass.Node_max = 100
	dataclass.Node_num = 0

	/* add the dataclass to the global list */
	ok := ListNode_insert_next(global_dataclass_entry, dataclass.List)
	if !ok {
		dataclass.List.Parent = nil
		dataclass.Node_list.Parent = nil
		dataclass.List = nil
		dataclass.Node_list = nil
		return nil
	}
	global_dataclass_num++

	return dataclass
}

func DataClass_find(id string) *DataClass {
	listnode := global_dataclass_entry

	if listnode.Next == listnode || listnode.Prev == listnode {
		return nil
	}

	if id == "last" {
		return listnode.Prev.Parent.(*DataClass)
	} else if id == "first" {
		return listnode.Next.Parent.(*DataClass)
	}

	for i := 0; i < global_dataclass_num; i++ {
		listnode = ListNode_index_node(listnode, 1)
		parent := listnode.Parent.(*DataClass)
		if parent.Id == id {
			return parent
		}
	}
	return nil
}

func (dc *DataClass) DataClass_unregister(ctx context.Context) bool {

	ok := ListNode_delete(dc.List)
	if !ok {
		return false
	}
	global_dataclass_num--

	return true
}

func (dc *DataClass) DataClass_add(datanode *DataNode) bool {
	if datanode == nil {
		return false
	}

	data_entry := dc.Node_list
	listnode := datanode.List
	ok := ListNode_insert_next(data_entry, listnode)
	if !ok {
		return false
	}
	dc.Node_num++
	return true
}

func (dc *DataClass) DataClass_remove(datanode *DataNode) bool {

	if datanode == nil {
		return false
	} else {
		ok := ListNode_delete(datanode.List)
		if !ok {
			return false
		}
		dc.Node_num--
	}
	return true
}

func (dc *DataClass) DataClass_search(id string) *DataNode {

	listnode := dc.Node_list

	if listnode.Next == listnode || listnode.Prev == listnode {
		return nil
	}

	if id == "last" {
		return listnode.Prev.Parent.(*DataNode)
	} else if id == "first" {
		return listnode.Next.Parent.(*DataNode)
	}

	for index := 0; index < dc.Node_num; index++ {
		listnode = ListNode_index_node(listnode, 1)
		parent := listnode.Parent.(*DataNode)
		if parent.Id == id {
			return parent
		}
	}
	return nil
}

type DataNode struct {
	List *ListNode /* it is a continer that is used to orgnize the parent type as a list */

	Id string

	Message interface{} /* the message type is interface{}, which make the DataNode holding all message type data */

	Payload []byte /* the Payload pointer is used to point the actual data */

	Timestamp time.Time /* Timestamp records the time instant which the DataNode hold the data at the time */
}

func DataNode_create(message interface{}, payload []byte, id string) *DataNode {

	datanode := new(DataNode)

	datanode.Id = id
	datanode.Message = message
	datanode.Payload = payload
	datanode.Timestamp = time.Now()
	datanode.List = ListNode_create(datanode)

	return datanode

}
