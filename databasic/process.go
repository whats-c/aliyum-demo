package databasic

import "context"

type ProceNode struct {
	List *ListNode /* it is a continer that is used to orgnize the parent type as a list */

	Id string

	Operation interface{} /* the Operation type is interface{}, which make the ProceNode holding all operation type method */

	Lock int /* it be used to prevent the competing, while the user to update the Operation */

	Class_list *ListNode /* the list hold all DataClass data, which hold all DataNode */
	Class_num  int       /* the member records the number of the Class_list length sub one */
	Class_max  int       /* the memeber is unused */
}

func ProceNode_register(operation interface{}, id string) *ProceNode {

	if operation == nil || global_procenode_num == MAX_PROCENODE_NUMBER {
		return nil
	}

	procenode := new(ProceNode)
	procenode.Id = id
	procenode.Lock = 0
	procenode.Operation = operation
	procenode.List = ListNode_create(procenode)
	procenode.Class_list = ListNode_create(procenode)
	procenode.Class_max = 100
	procenode.Class_num = 0

	/* add the procenode to the global list */
	entry := global_procenode_entry
	listnode := procenode.List

	ok := ListNode_insert_next(entry, listnode)
	if !ok {
		procenode.List.Parent = nil
		procenode.Class_list.Parent = nil
		procenode.Class_list = nil
		procenode.List = nil
		return nil
	}
	global_procenode_num++

	return procenode
}

func (pn *ProceNode) ProceNode_unregister(ctx context.Context) bool {

	ok := ListNode_delete(pn.List)
	if !ok {
		return false
	}
	global_procenode_num--

	return true
}

func ProceNode_find(id string) (procenode *ProceNode) {
	listnode := global_procenode_entry

	if listnode.Next == listnode || listnode.Prev == listnode {
		return nil
	}

	if id == "last" {
		return listnode.Prev.Parent.(*ProceNode)
	} else if id == "first" {
		return listnode.Next.Parent.(*ProceNode)
	}

	for i := 0; i < global_procenode_num; i++ {
		listnode = ListNode_index_node(listnode, 1)
		parent := listnode.Parent.(*ProceNode)
		if parent.Id == id {
			return parent
		}
	}
	return nil
}

func (pn *ProceNode) ProceNode_update_method(operation interface{}) bool {
	if operation == nil {
		return false
	} else {
		pn.Operation = operation
		return true
	}
}

func (pn *ProceNode) ProceNode_update_id(id string) bool {
	if id == "" {
		return false
	} else {
		pn.Id = id
		return true
	}
}

func (pn *ProceNode) ProceNode_lock() bool {
	if pn.Lock == 1 {
		return false
	} else {
		pn.Lock = 1
		return true
	}

}

func (pn *ProceNode) ProceNode_unlock() (ok int) {
	if pn.Lock == 0 {
		ok = 1
	} else {
		pn.Lock = 0
		ok = 1
	}
	return ok
}

func (pn *ProceNode) ProceNode_search(id string) *DataClass {
	listnode := pn.Class_list

	if listnode.Next == listnode || listnode.Prev == listnode {
		return nil
	}

	if id == "last" {
		return listnode.Prev.Parent.(*DataClass)
	} else if id == "first" {
		return listnode.Next.Parent.(*DataClass)
	}

	for index := 0; index < pn.Class_num; index++ {
		listnode = ListNode_index_node(listnode, 1)
		parent := listnode.Parent.(*DataClass)
		if parent.Id == id {
			return parent
		}
	}
	return nil
}

func (pn *ProceNode) ProceNode_add(dataclass *DataClass) bool {
	if dataclass == nil {
		return false
	}

	data_entry := pn.Class_list
	listnode := dataclass.List
	ok := ListNode_insert_next(data_entry, listnode)
	if !ok {
		return false
	}
	pn.Class_num++

	return true
}

func (pn *ProceNode) ProceNode_remove(dataclass *DataClass) bool {
	if dataclass == nil {
		return false
	} else {
		ok := ListNode_delete(dataclass.List)
		if !ok {
			return false
		}
		pn.Class_num--
	}
	return true
}
