package main

import (
	"context"
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/thb-cmyk/aliyum-demo/amqpbasic"
	"github.com/thb-cmyk/aliyum-demo/databasic"

	"github.com/thb-cmyk/aliyum-demo/utils"
)

/* the structure is container, which can store the information extracting from json stream */
type MessageStructure struct {
	Params map[string]interface{} `json:"params"`
}

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
	data, index := session.ReceiverData(linkid, num)
	if index != 0 {
		for i := 0; i < index; i++ {
			fmt.Printf("%s\n\r", data[i])
			var cms MessageStructure
			err := json.Unmarshal(data[i], &cms)
			log.Printf("%v\n\r", cms.Params)
			if err != nil {
				fmt.Print(err.Error())
			} else {
				raw_node := databasic.RawNode_create("aliyun", cms.Params)
				databasic.Send_raw(raw_node)
			}
		}
	}
}

/*
create a processor to handle the received data from aliyun amqp server,we should registry it to databasic
*/
func dataProccessor(tasknode *databasic.TaskNode, rawnode *databasic.RawNode) bool {
	value := rawnode.Raw.(map[string]interface{})
	for key, value := range value {
		switch key {
		case "voltage":
			result, err := VoltageInsert(value.(string))
			if err != nil {
				fmt.Print(err.Error())
			} else {
				id, _ := result.LastInsertId()
				num, _ := result.RowsAffected()
				fmt.Printf("effected rows: %d, last rows id: %d\n\r", num, id)
			}
			return true
		case "check_mode":
			result, err := CheckModeInsert(int(value.(float64)))
			if err != nil {
				fmt.Print(err.Error())
			} else {
				id, _ := result.LastInsertId()
				num, _ := result.RowsAffected()
				fmt.Printf("effected rows: %d, last rows id: %d\n\r", num, id)
			}
			return true
		case "error_info":
			result, err := ErrorInfoInsert(int(value.(float64)))
			if err != nil {
				fmt.Print(err.Error())
			} else {
				id, _ := result.LastInsertId()
				num, _ := result.RowsAffected()
				fmt.Printf("effected rows: %d, last rows id: %d\n\r", num, id)
			}
			return true
		default:
		}

	}
	return false
}
