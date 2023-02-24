package main

import (
	"testing"
	"time"
)

func TestRegisterDevice(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtua3AFCKMl"
	devicename := "cloud-test001"
	nickname := "cloud_test001"
	resp, err := RegisterDevice(client, &productkey, &iotinstanceid, &devicename, &nickname)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", resp.GoString())

}

func TestBatchRegisterDevice(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtua3AFCKMl"

	var count int32 = 5
	applyid, err := BatchRegisterDevice(client, &productkey, &count, &iotinstanceid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %d\n\r", *applyid)

	time.Sleep(time.Millisecond * 1000)

	resp, err := QueryBatchRegisterDeviceStatus(client, &productkey, applyid, &iotinstanceid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", resp.GoString())

	qpairesp, err := QueryPageByApplyId(client, applyid, &iotinstanceid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", qpairesp.GoString())

}

func TestGetDeviceState(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtua3AFCKMl"
	devicename := "cloud-test001"

	gdsresp, err := GetDeviceStatus(client, &iotinstanceid, &productkey, &devicename, nil)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", gdsresp.GoString())

}

func TestBatchGetDeviceState(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtua3AFCKMl"
	devicename := "cloud-test001"
	iotid := "VnWFa50dKpjgLsJk5iU6gtua00"

	gdsresp, err := BatchGetDeviceState(client, &iotinstanceid, &productkey, &devicename, nil)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", gdsresp.GoString())

	gdsresp001, err := BatchGetDeviceState(client, &iotinstanceid, &productkey, &devicename, &iotid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", gdsresp001.GoString())
}

func TestQueryDevice(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtua3AFCKMl"

	qdresp, err := QueryDevice(client, &productkey, &iotinstanceid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", qdresp.GoString())

}

func TestDeleteDevice(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtua3AFCKMl"
	devicename := "cloud-test001"

	err = DeleteDevice(client, &productkey, &devicename, &iotinstanceid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}

}

func TestQueryDeviceDetail(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtua3AFCKMl"
	devicename := "cloud-test001"
	iotid := "EhCUHETZlvRQV4LKiJ2Xgtua00"

	qddresp, err := QueryDeviceDetail(client, &productkey, &devicename, nil, &iotinstanceid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", qddresp.GoString())

	qddresp, err = QueryDeviceDetail(client, &productkey, &devicename, &iotid, &iotinstanceid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", qddresp.GoString())

}

func TestQueryDevicePropertiesData(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtuaomfN3mH"
	devicename := "cloud-test001"
	//iotid := "EhCUHETZlvRQV4LKiJ2Xgtua00"
	identifer := "LightStatus"
	pagesize := "10"
	apsc := "1"
	starttime := "1579249499000"
	endtime := "1579249499999"

	qdpdresp, err := QueryDevicePropertiesData(client, &iotinstanceid, &productkey, &devicename, nil, &identifer, &starttime, &endtime, &apsc, &pagesize)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("The information: %s\n\r", qdpdresp.GoString())
}
