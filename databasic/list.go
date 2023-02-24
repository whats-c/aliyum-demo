package databasic

/* The ListNode type as a continer type is used to hold some value
that can to orgnize this value as a list. */
type ListNode struct {
	Prev   *ListNode
	Next   *ListNode
	Parent interface{}
}

/* The method return the data that pointed by the ln.Prev and is the interface{} type */
func (ln *ListNode) ListNode_lookhead() *ListNode {
	return ln.Prev
}

/* The method return the data that pointed by the ln.Next and is the interface{} type */
func (ln *ListNode) ListNode_lookback() *ListNode {
	return ln.Next
}

/* The functoin aims to insert a node to a list and the location is the front of listnode
Note: the unique cause of resulting error is the list or node empty. */
func ListNode_insert_prev(list *ListNode, node *ListNode) bool {
	if list == nil || node == nil {
		return false
	}
	node_temp := list.Prev
	list.Prev = node
	node_temp.Next = node
	node.Prev = node_temp
	node.Next = list
	return true
}

/* The function is aim to insert a node the list and the location is the front of listnode
Note: the unique cause of resulting error is the list or node is empty. */
func ListNode_insert_next(list *ListNode, node *ListNode) bool {
	if list == nil || node == nil {
		return false
	}
	node_temp := list.Next
	list.Next = node
	node_temp.Prev = node
	node.Prev = list
	node.Next = node_temp
	return true
}

/* The function delete a node from a list
Note: you should pass a valid node to the function that valid data is noe nil. */
func ListNode_delete(node *ListNode) bool {
	if node == nil {
		return false
	} else {
		node_prev := node.Prev
		node_next := node.Next
		node_prev.Next = node_next
		node_next.Prev = node_prev
		node.Next = nil
		node.Prev = nil
	}
	return true
}

/* The function is to search a node which location is original node location add index */
func ListNode_index_node(node *ListNode, index int) *ListNode {
	listnode := node
	var index_temp int = 0

	if index > 0 {
		for ; index_temp < index; index_temp++ {
			listnode = listnode.Next
		}
	} else if index < 0 {
		for ; index_temp+index < 0; index_temp++ {
			listnode = listnode.Prev
		}
	}

	return listnode
}

/* creating a node typed ListNode */
func ListNode_create(parent interface{}) *ListNode {
	node := new(ListNode)
	if node == nil {
		return nil
	}
	node.Parent = parent
	node.Next = node
	node.Prev = node

	return node
}
