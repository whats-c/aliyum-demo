package databasic

import (
	"fmt"
	"testing"
)

func TestBroker(t *testing.T) {
	All_Init()
	Broker()

	ProceNode_register(func(tasknode *TaskNode, rawnode *RawNode) bool {
		fmt.Printf("tasknode:%s, rawnode:%s\n\r", tasknode.Id, rawnode.Id)
		fmt.Printf("data:%s\n\r", rawnode.Raw.(string))
		return true
	}, "test001")

	ProceNode_register(func(tasknode *TaskNode, rawnode *RawNode) bool {
		fmt.Printf("tasknode:%s, rawnode:%s\n\r", tasknode.Id, rawnode.Id)
		fmt.Printf("data:%s\n\r", rawnode.Raw.(string))
		return true
	}, "test002")
	ProceNode_register(func(tasknode *TaskNode, rawnode *RawNode) bool {
		fmt.Printf("tasknode:%s, rawnode:%s\n\r", tasknode.Id, rawnode.Id)
		fmt.Printf("data:%s\n\r", rawnode.Raw.(string))
		return true
	}, "test003")

	go func() {
		for i := 0; i < 60; i++ {
			data := fmt.Sprintf("rawnode data %d", i)
			rawnode := RawNode_create("test001", data)
			Send_raw(rawnode)
			if i == 59 {
				i = 0
			}
		}
	}()

	go func() {
		for i := 0; i < 60; i++ {
			data := fmt.Sprintf("rawnode data %d", i)
			rawnode := RawNode_create("test002", data)
			Send_raw(rawnode)
			if i == 59 {
				i = 0
			}
		}

	}()
	go func() {
		for i := 0; i < 60; i++ {
			data := fmt.Sprintf("rawnode data %d", i)
			rawnode := RawNode_create("test003", data)
			Send_raw(rawnode)
			if i == 59 {
				i = 0
			}
		}
	}()

	for {
		fmt.Printf("")
	}


}
