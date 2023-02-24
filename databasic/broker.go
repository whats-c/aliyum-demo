package databasic

import (
	"context"
	"fmt"
	"log"
	"time"
)

/* the global channel receive all data from all receiving go routine */
var global_raw_channel chan *RawNode

/* the channel is global channel, which making the privileger receiving control information from router, scheduler, controler... */
var main_monitor_channel chan *Monitor

/* the node is the global list entry, You can search all DataClass instance of existing base on the node. */
var global_dataclass_entry *ListNode
var global_dataclass_num int

/* the node is the global list entry. You can search all ProceNode instance of existing base on the node. */
var global_procenode_entry *ListNode
var global_procenode_num int

/* the node is the global list entry. You can search all TaskNode instance of existing base on the node */
var global_tasknode_entry *ListNode
var global_tasknode_num int

const (
	MAX_TASKNODE_NUMBER  int = 10
	MAX_PROCENODE_NUMBER int = 10
	MAX_DATACLASS_NUMBER int = 100
	MAX_RAWNODE_NUMBER   int = 100
)

/* the following const should be cared by user */
const (
	DEFAULT_MONITOR_SIZE   int           = 200
	DEFAULT_TIMEPEICE      time.Duration = time.Millisecond
	DEFAUT_SLEEP_TIMEPEICE time.Duration = time.Millisecond
	DEFAULT_TIMEOUT        time.Duration = time.Millisecond
)

const (
	Add        int = 1
	Remove     int = 2
	Update     int = 3
	Unregister int = 4
	Perform    int = 5
)

func Broker() {
	/* the unique functionality of broker is to create router, scheduler, controler, privileger go routine */
	go Router()
	go Scheduler()
	// we should create a go routine based on Controler(), but the conroler() is empty.
	//go Controler()
}

func Router() {

	for {
		/* receiving rawnode from global channel. */
		/* we should sleep to wait the rawnode, if the global channel is empty. */
		var rawnode *RawNode
		select {
		case rawnode = <-Receive_raw():
			/* we should to ignore the rawnode and receive next rawnode, if the rawnode id is empty */
			if rawnode.Id == "" {
				continue
			}
		default:
			/* sleeping go routine to wait rawnode */
			time.Sleep(DEFAUT_SLEEP_TIMEPEICE)
			continue
		}

		/* select a proper tasknode base on rawnode id */
		tasknode := TaskNode_find(rawnode.Id)
		/* we should to create a new tasknode, if the task list no matched tasknode */
		if tasknode == nil {
			/* select a proper procenode base on rawnode id */
			procenode := ProceNode_find(rawnode.Id)
			/* we should ignore the rawnode, if the procenode list no matched procenode */
			if procenode == nil {
				/* we should to handle the condition that a raw data receiving from global
				channel which is not capability to process */
				fmt.Printf("The process receiving from the global a raw data named %s that no capability to handler\n\r", rawnode.Id)
				continue
			} else {
				tasknode = TaskNode_register(rawnode.Id, procenode, DEFAULT_TIMEPEICE)
				if tasknode == nil {
					fmt.Printf("The task of aiming to process the raw data named %s unable to register!\n\r", rawnode.Id)
					continue
				}
			}
		}
		//
		/* 		monitor := Monitor_Create(tasknode, rawnode, Add)
		   		Send_mon(monitor) */

		// we will add a component that handle this rawnode of failed to push rawnode
		ok := tasknode.TaskNode_Push(rawnode)
		if !ok {
			log.Printf("The task of aiming to process the raw data named %s unable to push rawnode!\n\r", rawnode.Id)
		}
	}

}

func Scheduler() {
	for {
		/* select a proper tasknode to work. now we select the first tasknode */
		tasknode := TaskNode_find("first")
		if tasknode == nil {
			/* we print the information to handle the condition of the task list is empty */
			//fmt.Printf("The urgent task is not found due to the task list is empty!\n\r")
			time.Sleep(DEFAUT_SLEEP_TIMEPEICE)
			continue
		} else if tasknode.Goroutine {
			time.Sleep(DEFAUT_SLEEP_TIMEPEICE)
			continue
		}
		/* the method is forced convert to the type of func(*RawNode, *TaskNode) bool */
		method := tasknode.Method.Operation.(func(*TaskNode, *RawNode) bool)
		if method == nil {
			/* the branch is no possible arriving */
			continue
		}
		tasknode.Goroutine = true
		/* you should to consider the argument that the go routine that will be created required */
		go func(tasknode *TaskNode) {
			// we should to create a context by use the context.WithDeadLine(), if we want to add timepeice for the go routine
			ctx := context.TODO()

			for {
				select {
				// the case check if the context expity or not
				case <-ctx.Done():
					return
				// the case check if the task has canceled or not
				case <-tasknode.Cancel:
					return
				// the case check if have rawnode or not in task buffer
				default:
					rawnode := tasknode.TaskNode_Fetch()
					if rawnode == nil {
						time.Sleep(DEFAUT_SLEEP_TIMEPEICE)
						continue
					}
					ok := method(tasknode, rawnode)
					if !ok {
						fmt.Printf("in the task go routine %s, the method return a error!\n\r", tasknode.Id)
					} else {
						// Now, we dont to handle the condition
					}
				}

			}
		}(tasknode)
		/* move the tasknode to list hail */
		{
			listnode := tasknode.List
			ok := ListNode_delete(listnode)
			if !ok {
				fmt.Printf("")
			}
			ok = ListNode_insert_prev(global_tasknode_entry, listnode)
			if !ok {
				fmt.Printf("")
			}
		}
	}
}

func Controler() {

}

func Receive_raw() <-chan *RawNode {
	return global_raw_channel
}

func Send_raw(rawnode *RawNode) {
	global_raw_channel <- rawnode
}

func Send_mon(Monitor *Monitor) {
	main_monitor_channel <- Monitor
}

func Receive_mon() <-chan *Monitor {
	return main_monitor_channel
}

func All_Init() {
	/* initialize the global resource */
	global_dataclass_entry = ListNode_create(nil)
	global_dataclass_num = 0

	global_procenode_entry = ListNode_create(nil)
	global_procenode_num = 0

	global_tasknode_entry = ListNode_create(nil)
	global_tasknode_num = 0

	procenode := new(ProceNode)
	procenode.Id = "global"
	procenode.List = global_procenode_entry
	procenode.Lock = 1
	procenode.Operation = "global"

	dataclass := new(DataClass)
	dataclass.Id = "global"
	dataclass.List = global_dataclass_entry
	dataclass.Node_max = 100
	dataclass.Node_num = 0
	dataclass.Node_list = nil /* it is possible to make error */

	tasknode := new(TaskNode)
	tasknode.Id = "global"
	tasknode.Method = nil /* it is possible to make error */
	tasknode.Tiemout_is_set = false
	tasknode.Timeout = time.Now()
	tasknode.Timepeice = 0
	tasknode.Buffer = make(chan *RawNode, 100)
	tasknode.List = global_tasknode_entry

	global_dataclass_entry.Parent = dataclass
	global_procenode_entry.Parent = procenode
	global_tasknode_entry.Parent = tasknode

	/* initialize the global raw channel */
	global_raw_channel = make(chan *RawNode, MAX_RAWNODE_NUMBER)

	/* initialize the main monitor control channel */
	main_monitor_channel = make(chan *Monitor, DEFAULT_MONITOR_SIZE)

}
