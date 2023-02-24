package main

import (
	"testing"

	"github.com/thb-cmyk/aliyum-demo/databasic"
)

func TestInit(t *testing.T) {

	databasic.All_Init()

	databasic.Broker()

	databasic.ProceNode_register(dataProccessor, "LightStatus")

	MysqlInit()

	Aliyun_Connect()
}
