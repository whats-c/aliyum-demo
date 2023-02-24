package main

import (
	"fmt"

	openapi "github.com/alibabacloud-go/darabonba-openapi/client"
	console "github.com/alibabacloud-go/tea-console/client"

	"github.com/alibabacloud-go/tea/tea"

	iot "github.com/alibabacloud-go/iot-20180120/v3/client"

	util "github.com/alibabacloud-go/tea-utils/service"
)

/**
 * 使用AK&SK初始化账号Client
 * @param accessKeyId
 * @param accessKeySecret
 * @return Client
 * @throws Exception
 */
func CreateClient(accessKeyId *string, accessKeySecret *string) (_result *iot.Client, _err error) {
	config := &openapi.Config{
		// 您的AccessKey ID
		AccessKeyId: accessKeyId,
		// 您的AccessKey Secret
		AccessKeySecret: accessKeySecret,
	}
	// 访问的域名
	config.Endpoint = tea.String("iot.cn-shanghai.aliyuncs.com")
	_result = &iot.Client{}
	_result, _err = iot.NewClient(config)
	return _result, _err
}

/**
1.I modify the original code to prevent a condition occuring that the
client.CreateProduct(request) not return a error but the creating operation
is failed.
2.The modifed code is based on the idea which it can handle the condition
of above adding a peice of checking code
*/
func CreateProduct(client *iot.Client, iotInstanceId *string, regionId *string, nodeType *int32, productName *string, description *string, dataFormat *int32, categoryKey *string, netType *string, aliyuncommoditycode *string) (_result *string, _err error) {
	// 创建APi请求并获取参数
	request := &iot.CreateProductRequest{}
	request.IotInstanceId = iotInstanceId
	request.NodeType = nodeType
	request.ProductName = productName
	request.Description = description
	request.DataFormat = dataFormat
	request.CategoryKey = categoryKey
	request.NetType = netType
	request.AliyunCommodityCode = aliyuncommoditycode
	err, tryErr := func() (err error, _e error) {
		defer func() {
			if r := tea.Recover(recover()); r != nil {
				_e = r
			}
		}()
		resp, _err := client.CreateProduct(request)
		if _err != nil {
			return err, _err
		}
		/* It is possible the err is nil and creating product is failed */
		//console.Log(tea.String("creating product " + tea.StringValue(resp.Body.ProductKey) + " success"))
		//_result = resp.Body.ProductKey
		if !*resp.Body.Success {
			return fmt.Errorf("code: %s, error: %s", *resp.Body.Code, *resp.Body.ErrorMessage), _err
		}
		_result = resp.Body.ProductKey

		return nil, _err
	}()

	if tryErr != nil {
		var error = &tea.SDKError{}
		if _t, ok := tryErr.(*tea.SDKError); ok {
			error = _t
		} else {
			error.SetErrMsg(tryErr.Error())
		}
		console.Log(error.Message)
		_result = nil
		return _result, _err
	}

	return _result, err
}

func QueryProduct(client *iot.Client, iotinstanceid *string, productKey *string) (_result *iot.QueryProductResponse, _err error) {
	// 创建APi请求并获取参数
	request := &iot.QueryProductRequest{}
	request.ProductKey = productKey
	request.IotInstanceId = iotinstanceid

	err, tryErr := func() (err error, _e error) {
		defer func() {
			if r := tea.Recover(recover()); r != nil {
				_e = r
			}
		}()
		resp, _err := client.QueryProduct(request)
		if _err != nil {
			return err, _err
		}
		/* It is possible the err is nil and creating product is failed */
		if !*resp.Body.Success {
			return fmt.Errorf("code: %s, error: %s", *resp.Body.Code, *resp.Body.ErrorMessage), _err
		}
		_result = resp
		return err, _err
	}()

	if tryErr != nil {
		var error = &tea.SDKError{}
		if _t, ok := tryErr.(*tea.SDKError); ok {
			error = _t
		} else {
			error.SetErrMsg(tryErr.Error())
		}
		console.Log(error.Message)
		_result = nil
		return _result, _err
	}
	if err != nil {
		return _result, err
	}
	return _result, _err
}

func DeleteProduct(client *iot.Client, iotinstanceid *string, productKey *string) error {

	request := &iot.DeleteProductRequest{}
	request.ProductKey = productKey
	request.IotInstanceId = iotinstanceid
	err, tryErr := func() (err error, _e error) {
		defer func() {
			if r := tea.Recover(recover()); r != nil {
				_e = r
			}
		}()
		resp, _err := client.DeleteProduct(request)
		if _err != nil {
			return err, _err
		}

		/* It is possible the err is nil and creating product is failed */
		//body := resp.Body
		//console.Log(tea.String("删除公共产品调用成功。requestId: " + tea.StringValue(body.RequestId)))
		if !*resp.Body.Success {
			return fmt.Errorf("code: %s, error: %s", *resp.Body.Code, *resp.Body.ErrorMessage), _err
		}

		return err, _err
	}()

	if tryErr != nil {
		var error = &tea.SDKError{}
		if _t, ok := tryErr.(*tea.SDKError); ok {
			error = _t
		} else {
			error.Message = tea.String(tryErr.Error())
		}
		console.Log(error.Message)
	}
	if err != nil {
		return err
	}
	return tryErr
}

/**
 * 入参校验
 * param:系统规定参数.处理内容 Action - Not Null
 * param:产品KEY ProductKey - Not Null
 * param:Topic类ID TopicId
 * param:Topic类自定义类目名称 TopicShortName
 * param:操作权限 Operation
 * return 校验通过:true 校验失败:false
 */
func CheckParametersTopic(pAction *string, pProductKey *string, pTopicId *string, pTopicShortName *string, pOperation *string) (_result *bool) {
	// 系统规定参数:处理内容。
	if tea.BoolValue(util.Empty(pAction)) {
		console.Log(tea.String("==========入参[Action]不能为空=========="))
		_result = tea.Bool(false)
		return _result
	}

	deleteFlag := util.EqualString(tea.String("DeleteProductTopic"), pAction)
	createFlag := util.EqualString(tea.String("CreateProductTopic"), pAction)
	updateFlag := util.EqualString(tea.String("UpdateProductTopic"), pAction)
	queryFlag := util.EqualString(tea.String("QueryProductTopic"), pAction)
	if !tea.BoolValue(deleteFlag) && !tea.BoolValue(createFlag) && !tea.BoolValue(updateFlag) && !tea.BoolValue(queryFlag) {
		console.Log(tea.String("==========入参[Action]不能识别=========="))
		console.Log(tea.String("入参[Action]列表:CreateProductTopic,DeleteProductTopic,UpdateProductTopic,QueryProductTopic。"))
		_result = tea.Bool(false)
		return _result
	}

	if tea.BoolValue(createFlag) || tea.BoolValue(queryFlag) {
		// 产品的ProductKey。
		if tea.BoolValue(util.Empty(pProductKey)) {
			console.Log(tea.String("==========入参[ProductKey]不能为空=========="))
			_result = tea.Bool(false)
			return _result
		}

	}

	if tea.BoolValue(deleteFlag) || tea.BoolValue(updateFlag) {
		// Topic类ID
		if tea.BoolValue(util.Empty(pTopicId)) {
			console.Log(tea.String("==========入参[TopicId]不能为空=========="))
			_result = tea.Bool(false)
			return _result
		}

	}

	if tea.BoolValue(createFlag) || tea.BoolValue(updateFlag) {
		// Topic类自定义类目名称
		if tea.BoolValue(util.Empty(pTopicShortName)) {
			console.Log(tea.String("==========入参[TopicShortName]不能为空=========="))
			_result = tea.Bool(false)
			return _result
		}

		// 操作权限
		if tea.BoolValue(util.Empty(pOperation)) {
			console.Log(tea.String("==========入参[Operation]不能为空=========="))
			_result = tea.Bool(false)
			return _result
		}

	}

	// 入参校验通过
	_result = tea.Bool(true)
	return _result
}

func QueryProductTopic(client *iot.Client, pIotInstanceId *string, pProductKey *string) (_result *iot.QueryProductTopicResponseBodyData, _err error) {

	request := &iot.QueryProductTopicRequest{
		ProductKey:    pProductKey,
		IotInstanceId: pIotInstanceId,
	}

	response, _err := client.QueryProductTopic(request)
	if _err != nil {
		return nil, _err
	}

	if !*response.Body.Success {
		return nil, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}
	_result = response.Body.Data
	return _result, _err
}

func CreateProductTopic(client *iot.Client, pIotInstanceId *string, pProductKey *string, pOperation *string, pTopicShortName *string, pDesc *string) (_result *int64, _err error) {

	request := &iot.CreateProductTopicRequest{
		ProductKey:     pProductKey,
		Operation:      pOperation,
		TopicShortName: pTopicShortName,
		Desc:           pDesc,
		IotInstanceId:  pIotInstanceId,
	}

	response, _err := client.CreateProductTopic(request)
	if _err != nil {
		return nil, _err
	}

	if !*response.Body.Success {
		return nil, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}
	_result = response.Body.TopicId

	return _result, _err
}

func UpdateProductTopic(client *iot.Client, pIotInstanceId *string, pTopicId *string, pOperation *string, pTopicShortName *string, pDesc *string) (_err error) {

	request := &iot.UpdateProductTopicRequest{
		TopicId:        pTopicId,
		Operation:      pOperation,
		TopicShortName: pTopicShortName,
		Desc:           pDesc,
		IotInstanceId:  pIotInstanceId,
	}

	response, _err := client.UpdateProductTopic(request)
	if _err != nil {
		return _err
	}

	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err
}

func DeleteProductTopic(client *iot.Client, pIotInstanceId *string, pTopicId *string) (_err error) {
	request := &iot.DeleteProductTopicRequest{
		TopicId:       pTopicId,
		IotInstanceId: pIotInstanceId,
	}

	response, _err := client.DeleteProductTopic(request)
	if _err != nil {
		return _err
	}

	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err
}
