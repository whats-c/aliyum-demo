package main

import (
	"fmt"
	"testing"
)

func TestCreateProduct(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}

	/* the following code to create a product */

	region := "cn-shanghai"
	IotInstanceId := "iot-06z00bp0nwmb9tp"
	ProductName := "cloud-test"
	var NodeType int32 = 0
	var DataFormat int32 = 0
	Description := "The product aims to test the cloud develpement experence."
	AliyunCommodityCode := "iothub_senior"
	NetType := "CELLULAR"
	//AuthType := "secret"
	//var ValidateType int32 = 2
	category := "Lighting"
	cpresp, err := CreateProduct(client, &IotInstanceId, &region, &NodeType, &ProductName, &Description, &DataFormat, &category, &NetType, &AliyunCommodityCode)
	if err != nil {
		t.Error("Create Product is failed\n\r")
	}
	t.Logf("The information: %s\n\r", *cpresp)

}

func TestDeleteProduct(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	productkey := "gtuaDP4muM8"
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	err = DeleteProduct(client, &iotinstanceid, &productkey)
	if err != nil {
		fmt.Printf("error information: %s", err.Error())
		t.Error()
	}

}

func TestQueryProduct(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}

	productkey := "gtua5daFrI6"
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	resp, err := QueryProduct(client, &iotinstanceid, &productkey)
	if err != nil {
		t.Logf("error information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("resp data: %s\n\r", *resp.Body.Data)
}

func TestCreateTopic(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	productkey := "gtua3AFCKMl"
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	operation := "ALL"
	topicshortname := "productKey/deviceName/topicShortName"
	decription := "The topic is used to tset cloud api."
	topicid, err := CreateProductTopic(client, &iotinstanceid, &productkey, &operation, &topicshortname, &decription)
	if err != nil {
		t.Logf("create product topic function return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("creating a topic and its id is %d\n\r", *topicid)
}

func TestDeleteTopic(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	topicid := "21936366"
	err = DeleteProductTopic(client, &iotinstanceid, &topicid)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}

}

func TestQeuryTopic(t *testing.T) {
	accesskey := "LTAI5tDTMKSxBGCgnbWyaPvt"
	accesskeysecret := "3Vmfy9VcAjIdX8V8HQMzDdnxucfrm3"
	client, err := CreateClient(&accesskey, &accesskeysecret)
	if err != nil {
		t.Error()
	}
	iotinstanceid := "iot-06z00bp0nwmb9tp"
	productkey := "gtua3AFCKMl"
	data, err := QueryProductTopic(client, &iotinstanceid, &productkey)
	if err != nil {
		t.Logf("Delet product topic return a error!\n\r")
		t.Logf("The information: %s\n\r", err.Error())
		t.Error()
		return
	}
	t.Logf("Topic information: %s\n\r", data.GoString())
}
