package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/thb-cmyk/aliyum-demo/amqpbasic"
	"github.com/thb-cmyk/aliyum-demo/databasic"

	"github.com/thb-cmyk/aliyum-demo/utils"
)

/*
The function is used to intialize the amqp client connecting to aliyun amqp sever.
*/
func Aliyun_Connect() {

	/* patch the required information from the yaml configuration file */
	configmap := utils.GetYamlConfig("config/config.yaml")
	accessKey := utils.GetElement("accessKey", configmap)
	accessSecret := utils.GetElement("accessSecret", configmap)
	consumerGroupId := utils.GetElement("consumerGroupId", configmap)
	clientId := utils.GetElement("clientId", configmap)
	iotInstanceId := utils.GetElement("iotInstanceId", configmap)
	host := utils.GetElement("host", configmap)

	/* configure the parameters, which is neccessary to connect to aliyun amqp server */
	address := "amqps://" + host + ":5671"
	timestamp := time.Now().Nanosecond() / 1000000
	username := fmt.Sprintf("%s|authMode=aksign,signMethod=Hmacsha1,consumerGroupId=%s,authId=%s,iotInstanceId=%s,timestamp=%d|",
		clientId, consumerGroupId, accessKey, iotInstanceId, timestamp)
	stringToSign := fmt.Sprintf("authId=%s&timestamp=%d", accessKey, timestamp)
	hmacKey := hmac.New(sha1.New, []byte(accessSecret))
	hmacKey.Write([]byte(stringToSign))
	password := base64.StdEncoding.EncodeToString(hmacKey.Sum(nil))
	aliyun_session_id := amqpbasic.SessionIdentifyInit(address, username, password, "session001")
	aliyun_session := new(amqpbasic.AmqpSessionHandler)

	/* create a  root context */
	root_ctx := context.Background()

	/* create a session. if the bases client is not present, it will creat a client */
	/* if use the root_ctx, the function never return a timeout error */
	ok := aliyun_session.SessionInit(aliyun_session_id, 1, root_ctx)
	if ok == -1 {
		/* if use the root_ctx, the function never return a timeout error */
		fmt.Printf("The works of creating a new session is failed!\n\r")
		return
	}
	fmt.Printf("The works of creating a new session is successful!\n\r")

	/* create a link based to session_test */
	ok = aliyun_session.LinkCreate("receiver_voltage")
	if ok == -1 {
		fmt.Printf("The works of creating a new link is failed!\n\r")
		return
	}
	fmt.Printf("The works of creating a new link is successful!\n\r")

	/* create a daemon thread that receive data from the amqp server */
	go amqpbasic.ReceiveThread(root_ctx)

	/* prehandle the data receiving from amqp server and send the result to databasic */
	for {
		// prehandle the recevied data and send the result to databasic
		dataPreHandle(aliyun_session, "receiver_voltage", 1)

		time.Sleep(500 * time.Millisecond)
	}
}

/*
The function is used to prehandle the data receiving from aliyun amqp server. And creating
a raw node which contian the prehandled datato send to databasic.
*/
func dataPreHandle(session *amqpbasic.AmqpSessionHandler, linkid string, num int) {

	// the data Prehandle function can handle the device status update message and device data update message
	message, index := session.ReceiverMessage(linkid, num)
	if index != 0 {
		for i := 0; i < index; i++ {
			// get the topic of the message belong to
			topic := message[i].ApplicationProperties["topic"].(string)
			// jugde the message type and what to handle it
			// the device status update message topic model is "as/mqtt/status/${productKey}/${deviceName}"
			// the device data update message topic model is "/${productKey}/${deviceName}/user/update"
			if strings.Contains(topic, "as/mqtt/status") {
				// the device status update message
				deviceName := strings.Split(topic, "/")[5]
				fmt.Printf("topic: %s, deviceName: %s\n\r", topic, deviceName)
				// get the generate time of the message and convert it to yyyy-MM-dd HH:mm:ss SSS format
				generateTime := message[i].ApplicationProperties["generateTime"].(int64)
				fmt.Printf("generateTime: %d\n\r", generateTime)
				time := time.UnixMilli(generateTime)
				time_split := strings.Split(time.String(), " ")
				formattedTime := time_split[0] + "|" + time_split[1]
				fmt.Printf("formattedTime: %s\n\r", formattedTime)

				// get the message payload
				payload := message[i].GetData()

				// struct the message payload
				var gt GeneralStructure
				var ss StatusStructure
				err := json.Unmarshal(payload, &ss)
				if err != nil {
					fmt.Printf("json unmarshal error: %s\n\r", err)
					return
				} else {
					fmt.Printf("status: %s\n\r", ss.Status)
					value := ss.Status
					// create a general value structure
					vs := ValueStructure{
						Params: map[string]interface{}{
							"status": value},
					}
					log.Printf("%v\n\r", vs.Params)
					gt.DeviceName = deviceName
					gt.Time = formattedTime
					gt.Value = vs
					raw_node := databasic.RawNode_create("aliyun", &gt)
					databasic.Send_raw(raw_node)
				}

			} else if strings.Contains(topic, "/user/update") {
				// get the device name of the message belong to
				// the topic model is "/${productKey}/${deviceName}/user/update"
				deviceName := strings.Split(topic, "/")[2]
				fmt.Printf("topic: %s, deviceName: %s\n\r", topic, deviceName)

				// get the generate time of the message and convert it to yyyy-MM-dd HH:mm:ss SSS format
				generateTime := message[i].ApplicationProperties["generateTime"].(int64)
				fmt.Printf("generateTime: %d\n\r", generateTime)
				time := time.UnixMilli(generateTime)
				time_split := strings.Split(time.String(), " ")
				formattedTime := time_split[0] + "|" + time_split[1]
				fmt.Printf("formattedTime: %s\n\r", formattedTime)
				// get the message payload
				payload := message[i].GetData()

				// struct the message payload
				var gt GeneralStructure
				var vs ValueStructure
				err := json.Unmarshal(payload, &vs)
				if err != nil {
					fmt.Print(err.Error())
				} else {
					log.Printf("%v\n\r", vs.Params)
					gt.DeviceName = deviceName
					gt.Time = formattedTime
					gt.Value = vs
					raw_node := databasic.RawNode_create("aliyun", &gt)
					databasic.Send_raw(raw_node)
				}
			} else {
				fmt.Printf("The message topic is not correct!\n\r")
				fmt.Printf("topic: %s\n\r", topic)
			}

		}
	}

	// messages, index := session.ReceiverMessage(linkid, num)
	// for i := 0; i < index; i++ {
	// 	fmt.Printf("data: %v\n\r", messages[i])
	// }

	// data, index := session.ReceiverData(linkid, num)
	// for i := 0; i < index; i++ {
	// 	fmt.Printf("data: %s\n\r", data[i])
	// }
}

/*
create a processor to handle the received data from aliyun amqp server,we should registry it to databasic
*/
func dataProccessor(tasknode *databasic.TaskNode, rawnode *databasic.RawNode) bool {

	gt := rawnode.Raw.(*GeneralStructure)
	result, err := Insert(*gt)
	if err != nil {
		fmt.Print(err.Error())
		return false
	}
	id, _ := result.LastInsertId()
	num, _ := result.RowsAffected()
	fmt.Printf("effected rows: %d, last rows id: %d\n\r", num, id)
	return true

	// info := rawnode.Raw.(*GeneralStructure)
	// data := info.Value.Params

	// for key, value := range data {
	// 	switch key {
	// 	case "voltage":
	// 		var voltage VoltageStructure
	// 		voltage.Voltage = value.(float64)
	// 		voltage.DeviceName = info.DeviceName
	// 		voltage.Time = info.Time
	// 		result, err := Insert(voltage)
	// 		if err != nil {
	// 			fmt.Print(err.Error())
	// 		} else {
	// 			id, _ := result.LastInsertId()
	// 			num, _ := result.RowsAffected()
	// 			fmt.Printf("effected rows: %d, last rows id: %d\n\r", num, id)
	// 		}
	// 		return true
	// 	case "check_mode":
	// 		var check_mode CheckModeStructure
	// 		check_mode.CheckMode = int(value.(float64))
	// 		check_mode.DeviceName = info.DeviceName
	// 		check_mode.Time = info.Time
	// 		result, err := Insert(check_mode)
	// 		if err != nil {
	// 			fmt.Print(err.Error())
	// 		} else {
	// 			id, _ := result.LastInsertId()
	// 			num, _ := result.RowsAffected()
	// 			fmt.Printf("effected rows: %d, last rows id: %d\n\r", num, id)
	// 		}
	// 		return true
	// 	case "error_info":
	// 		var error_info ErrorInfoStructure
	// 		error_info.ErrorInfo = int(value.(float64))
	// 		error_info.DeviceName = info.DeviceName
	// 		error_info.Time = info.Time
	// 		result, err := Insert(error_info)
	// 		if err != nil {
	// 			fmt.Print(err.Error())
	// 		} else {
	// 			id, _ := result.LastInsertId()
	// 			num, _ := result.RowsAffected()
	// 			fmt.Printf("effected rows: %d, last rows id: %d\n\r", num, id)
	// 		}
	// 		return true
	// 	default:
	// 	}

}
