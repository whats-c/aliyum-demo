package main

import (
	"log"

	"github.com/thb-cmyk/aliyum-demo/databasic"
)

// configure loger for the project
func logConfig() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	log.SetPrefix("[log info:]")
}

func main() {

	logConfig()

	databasic.All_Init()

	databasic.Broker()

	databasic.ProceNode_register(dataProccessor, "aliyun")

	MysqlInit()

	defer MysqlDeInit()

	go Aliyun_Connect()

	// the following processer node is used to handle the http request
	databasic.ProceNode_register(voltageProccesser, "voltage")
	databasic.ProceNode_register(checkmodeProccesser, "check_mode")
	databasic.ProceNode_register(errorinfoProccesser, "error_info")
	databasic.ProceNode_register(statusProccesser, "status")

	IntrefaceInit()

}
