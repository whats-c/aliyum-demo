package databasic

type Monitor struct {
	Operation   int
	Information interface{}
	Tasknode    *TaskNode
}

func Monitor_Create(tasknode *TaskNode, information interface{}, operation int) *Monitor {
	monitor := new(Monitor)

	monitor.Tasknode = tasknode
	monitor.Information = information
	monitor.Operation = operation

	return monitor
}
