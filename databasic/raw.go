package databasic

type RawNode struct {
	Id     string
	Raw    interface{} /* the Raw type is interface{}, which make RawNode can hold all data type */
	List   *ListNode   /* it is a continer that is used to orgnize the parent type as a list */
	handle bool
}

func RawNode_create(id string, raw interface{}) *RawNode {
	rawnode := new(RawNode)

	rawnode.Id = id
	rawnode.Raw = raw
	rawnode.List = ListNode_create(rawnode)
	rawnode.handle = false

	return rawnode
}
