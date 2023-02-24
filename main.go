package main

import (
	"github.com/thb-cmyk/aliyum-demo/databasic"
)

func main() {

	databasic.All_Init()

	databasic.Broker()

	// databasic.ProceNode_register(dataProccessor, "voltage")
	// databasic.ProceNode_register(dataProccessor, "check_mode")
	// databasic.ProceNode_register(dataProccessor, "error_info")

	databasic.ProceNode_register(dataProccessor, "aliyun")

	MysqlInit()

	defer MysqlDeInit()

	go Aliyun_Connect()

	// the following processer node is used to handle the http request
	databasic.ProceNode_register(voltageProccesser, "voltage")
	databasic.ProceNode_register(checkmodeProccesser, "check_mode")
	databasic.ProceNode_register(errorinfoProccesser, "error_info")

	IntrefaceInit()

}
