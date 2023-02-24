# data.go
The data.go include two structure types dataclass and datanode. The relation between dataclass and datanode is that datanode is mounted to dataclass. And the dataclass instance is mounted to procecce node to record the handled data by the process node. But the dataclass and datanode types is not used by the project.

# monitor.go
The monitor.go include one types Monitor. But it's function is missed.

# list.go
The list.go includes one types listnode. It is used by other types included in project. For example procenode, dataclass, datanode etc. It's function is to listing all the instances that have the eaqual type.

# raw.go
The raw.go include one types rawnode. It is the basic element to handle the received data from other components. It includes the raw data will be handled.
While the other components hope to handle data by the process node, it should create the rawnode to containe the raw data.

# process.go
The process.go include one types procenode. It's core is operation element, which can handle the received data. 

# broker.go
The broker.go implements the broker functions. It consist of router, scheduler, and
controler components. The router receive data required to handle by the databasic, which form is rawnode. The scheduler schedule the registered task to handle data.
Now, the controler is not implemented.