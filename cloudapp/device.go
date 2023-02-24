package main

import (
	"fmt"

	number "github.com/alibabacloud-go/darabonba-number/client"
	string_ "github.com/alibabacloud-go/darabonba-string/client"
	time "github.com/alibabacloud-go/darabonba-time/client"
	iot "github.com/alibabacloud-go/iot-20180120/v3/client"
	console "github.com/alibabacloud-go/tea-console/client"
	util "github.com/alibabacloud-go/tea-utils/service"
	"github.com/alibabacloud-go/tea/tea"
)

func RegisterDevice(client *iot.Client, productKey *string, iotInstanceId *string, deviceName *string, nickName *string) (_result *iot.RegisterDeviceResponseBodyData, _err error) {
	request := &iot.RegisterDeviceRequest{
		ProductKey:    productKey,
		IotInstanceId: iotInstanceId,
		DeviceName:    deviceName,
		Nickname:      nickName,
	}
	response, _err := client.RegisterDevice(request)
	if _err != nil {
		return nil, _err
	}

	if !*response.Body.Success {
		return nil, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	_result = response.Body.Data
	return _result, _err
}

func BatchRegisterDevice(client *iot.Client, productKey *string, count *int32, iotInstanceId *string) (_result *int64, _err error) {
	request := &iot.BatchRegisterDeviceRequest{
		ProductKey:    productKey,
		Count:         count,
		IotInstanceId: iotInstanceId,
	}
	response, _err := client.BatchRegisterDevice(request)
	if _err != nil {
		return _result, _err
	}

	if !*response.Body.Success {
		return nil, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	_result = response.Body.Data.ApplyId
	return _result, _err
}

func QueryBatchRegisterDeviceStatus(client *iot.Client, productKey *string, applyId *int64, iotInstanceId *string) (_result *iot.QueryBatchRegisterDeviceStatusResponseBodyData, _err error) {
	request := &iot.QueryBatchRegisterDeviceStatusRequest{
		ApplyId:       applyId,
		ProductKey:    productKey,
		IotInstanceId: iotInstanceId,
	}
	response, _err := client.QueryBatchRegisterDeviceStatus(request)
	if _err != nil {
		return nil, _err
	}

	if !*response.Body.Success {
		return nil, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	_result = response.Body.Data

	return _result, _err
}

func QueryPageByApplyId(client *iot.Client, applyId *int64, iotInstanceId *string) (_result *iot.QueryPageByApplyIdResponseBody, _err error) {
	request := &iot.QueryPageByApplyIdRequest{
		ApplyId:       applyId,
		IotInstanceId: iotInstanceId,
	}
	response, _err := client.QueryPageByApplyId(request)
	if _err != nil {
		return nil, _err
	}

	if !*response.Body.Success {
		return nil, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}
	_result = response.Body

	return _result, _err
}

func GetDeviceStatus(client *iot.Client, iotinstance *string, productkey *string, devicename *string, iotId *string) (_result *iot.GetDeviceStatusResponseBodyData, _err error) {
	request := &iot.GetDeviceStatusRequest{}
	/* If the iotid is empty, we only to use the iotid */
	if iotId != nil {
		request.IotInstanceId = iotinstance
		request.IotId = iotId
	} else {
		request.IotInstanceId = iotinstance
		request.ProductKey = productkey
		request.DeviceName = devicename
	}
	response, _err := client.GetDeviceStatus(request)
	if _err != nil {
		return _result, _err
	}

	if !*response.Body.Success {
		return _result, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	_result = response.Body.Data
	return _result, _err
}

func BatchGetDeviceState(client *iot.Client, iotinstanceid *string, productKey *string, deviceNames *string, iotIds *string) (_result *iot.BatchGetDeviceStateResponseBodyDeviceStatusList, _err error) {
	// 创建请求接口
	err, tryErr := func() (err error, _e error) {
		defer func() {
			if r := tea.Recover(recover()); r != nil {
				_e = r
			}
		}()
		request := &iot.BatchGetDeviceStateRequest{}
		// 要查看运行状态的设备所隶属的产品Key。
		// 说明 如果传入该参数，需同时传入 DeviceNames。
		// 组建DeviceNames参数   要查看运行状态的设备的名称
		// 如果传入该参数，需同时传入ProductKey。单次查询最多50个设备。
		if !tea.BoolValue(util.Empty(deviceNames)) && !tea.BoolValue(util.Empty(productKey)) {
			request.IotInstanceId = iotinstanceid
			request.ProductKey = productKey
			deviceNameList := string_.Split(deviceNames, tea.String(","), tea.Int(50))
			request.DeviceName = deviceNameList
		}

		// 组建IotIds参数  要查看运行状态的设备ID列表。
		// 如果传入该参数，则无需传入 ProductKey和 DeviceName。
		// IotId作为设备唯一标识符，与 ProductKey 和 DeviceName组合是一一对应的关系。
		// 如果您同时传入 IotId和 ProductKey与 DeviceName组合，则以 IotId为准。
		if !tea.BoolValue(util.Empty(iotIds)) {
			request.IotInstanceId = iotinstanceid
			iotList := string_.Split(iotIds, tea.String(","), tea.Int(50))
			request.IotId = iotList
		}

		resp, _err := client.BatchGetDeviceState(request)
		if _err != nil {
			return err, _err
		}

		if !*resp.Body.Success {
			return err, fmt.Errorf("code: %s, error message: %s", *resp.Body.Code, *resp.Body.ErrorMessage)
		}
		_result = resp.Body.DeviceStatusList

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
		return _result, err
	}
	return _result, _err
}

func GetDeviceShadow(client *iot.Client, deviceName *string, productKey *string) (_result *string, _err error) {

	request := &iot.GetDeviceShadowRequest{}
	// 要查询的设备所隶属的产品Key
	request.ProductKey = productKey
	// 要查询的设备名称
	request.DeviceName = deviceName
	response, _err := client.GetDeviceShadow(request)
	if _err != nil {
		return _result, _err
	}

	if !*response.Body.Success {
		return _result, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	_result = response.Body.ShadowMessage

	return _result, _err
}

func DeleteDevice(client *iot.Client, productKey *string, deviceName *string, iotInstanceId *string) (_err error) {
	request := &iot.DeleteDeviceRequest{
		ProductKey:    productKey,
		DeviceName:    deviceName,
		IotInstanceId: iotInstanceId,
	}
	response, _err := client.DeleteDevice(request)
	if _err != nil {
		return _err
	}

	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err

}

func QueryDevice(client *iot.Client, productKey *string, iotInstanceId *string) (_result *iot.QueryDeviceResponseBodyData, _err error) {
	request := &iot.QueryDeviceRequest{
		ProductKey:    productKey,
		IotInstanceId: iotInstanceId,
	}
	response, _err := client.QueryDevice(request)
	if _err != nil {
		return _result, _err
	}
	if !*response.Body.Success {
		return _result, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	_result = response.Body.Data

	return _result, _err
}

func QueryDeviceDetail(client *iot.Client, pProductKey *string, pDeviceName *string, pIotId *string, pIotInstanceId *string) (_result *iot.QueryDeviceDetailResponseBodyData, _err error) {
	request := &iot.QueryDeviceDetailRequest{
		ProductKey:    pProductKey,
		DeviceName:    pDeviceName,
		IotId:         pIotId,
		IotInstanceId: pIotInstanceId,
	}

	response, _err := client.QueryDeviceDetail(request)
	if _err != nil {
		return _result, _err
	}

	if !*response.Body.Success {
		return _result, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	_result = response.Body.Data

	return _result, _err
}

func BatchQueryDeviceDetail(client *iot.Client, pProductKey *string, pDeviceName *string, pIotInstanceId *string) (_result *iot.BatchQueryDeviceDetailResponseBodyData, _err error) {
	deviceNameArray := string_.Split(pDeviceName, tea.String(","), tea.Int(101))
	request := &iot.BatchQueryDeviceDetailRequest{
		ProductKey:    pProductKey,
		DeviceName:    deviceNameArray,
		IotInstanceId: pIotInstanceId,
	}

	response, _err := client.BatchQueryDeviceDetail(request)
	if _err != nil {
		return _result, _err
	}

	if !*response.Body.Success {
		return _result, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	_result = response.Body.Data

	return _result, _err
}

/**
 * 获取主处理main的入参元素值，未设置时返回空字符串。
 * param:主处理main的入参数组
 * param:主处理main的入参下标
 * return:指定下标对应的具体入参内容，未设置则返回空字符串。
 */
/* func GetArg(args []*string, pIndex *int) (_result *string) {
	length := number.Itol(array.Size(args))
	if tea.BoolValue(number.Gt(length, number.Itol(pIndex))) {
		return _result
	}

	_result = tea.String("")
	return _result
} */

func QueryDevicePropertiesData(client *iot.Client, pIotInstanceId *string, pProductKey *string, pDeviceName *string, pIotId *string, pIdentifier *string, pStartTime *string, pEndTime *string, pAsc *string, pPageSize *string) (_result *iot.QueryDevicePropertiesDataResponseBodyPropertyDataInfos, _err error) {
	// 属性记录的开始时间
	// (北京时间2022-01-01 10:00:00)
	lStartTime := tea.Int64(1641002400000)
	if !tea.BoolValue(util.Empty(pStartTime)) {
		lStartTime = number.ParseLong(pStartTime)
	}

	// 属性记录的结束时间(当前时间/毫秒)
	lEndTime := number.ParseLong(time.Unix())
	if !tea.BoolValue(util.Empty(pEndTime)) {
		lEndTime = number.ParseLong(pEndTime)
	}

	// 属性记录按时间排序的方式
	iAsc := tea.Int(1)
	if !tea.BoolValue(util.Empty(pAsc)) {
		iAsc = number.ParseInt(pAsc)
	}

	// 单个属性可返回的数据记录数量
	iPageSize := tea.Int(100)
	if !tea.BoolValue(util.Empty(pPageSize)) {
		iPageSize = number.ParseInt(pPageSize)
	}

	request := &iot.QueryDevicePropertiesDataRequest{
		IotInstanceId: pIotInstanceId,
		StartTime:     lStartTime,
		EndTime:       lEndTime,
		Asc:           tea.ToInt32(iAsc),
		PageSize:      tea.ToInt32(iPageSize),
	}
	// 属性的标识符列表
	if !tea.BoolValue(util.Empty(pIdentifier)) {
		request.Identifier = string_.Split(pIdentifier, tea.String(","), tea.Int(1000))
	}

	if !tea.BoolValue(util.Empty(pIotId)) {
		request.IotId = pIotId
	} else {
		request.ProductKey = pProductKey
		request.DeviceName = pDeviceName
	}

	response, _err := client.QueryDevicePropertiesData(request)
	if _err != nil {
		return _result, _err
	}

	if !*response.Body.Success {
		return _result, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}
	_result = response.Body.PropertyDataInfos
	return _result, _err
}

func QueryDevicePropertyData(client *iot.Client, pIotInstanceId *string, pProductKey *string, pDeviceName *string, pIotId *string, pIdentifier *string, pStartTime *string, pEndTime *string, pAsc *string, pPageSize *string) (_result *iot.QueryDevicePropertyDataResponseBody, _err error) {
	// 属性记录的开始时间
	// (北京时间2022-01-01 10:00:00)
	lStartTime := tea.Int64(1641002400000)
	if !tea.BoolValue(util.Empty(pStartTime)) {
		lStartTime = number.ParseLong(pStartTime)
	}

	// 属性记录的结束时间(当前时间/毫秒)
	lEndTime := number.ParseLong(time.Unix())
	if !tea.BoolValue(util.Empty(pEndTime)) {
		lEndTime = number.ParseLong(pEndTime)
	}

	// 属性记录按时间排序的方式
	iAsc := tea.Int(1)
	if !tea.BoolValue(util.Empty(pAsc)) {
		iAsc = number.ParseInt(pAsc)
	}

	// 单个属性可返回的数据记录数量
	iPageSize := tea.Int(100)
	if !tea.BoolValue(util.Empty(pPageSize)) {
		iPageSize = number.ParseInt(pPageSize)
	}

	request := &iot.QueryDevicePropertyDataRequest{
		IotInstanceId: pIotInstanceId,
		Identifier:    pIdentifier,
		StartTime:     lStartTime,
		EndTime:       lEndTime,
		Asc:           tea.ToInt32(iAsc),
		PageSize:      tea.ToInt32(iPageSize),
	}
	if !tea.BoolValue(util.Empty(pIotId)) {
		request.IotId = pIotId
	} else {
		request.ProductKey = pProductKey
		request.DeviceName = pDeviceName
	}

	response, _err := client.QueryDevicePropertyData(request)
	if _err != nil {
		return _result, _err
	}
	if !*response.Body.Success {
		return _result, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}
	_result = response.Body

	return _result, _err
}

func QueryDevicePropertyStatus(client *iot.Client, pIotInstanceId *string, pProductKey *string, pDeviceName *string, pIotId *string, pFunctionBlockId *string) (_result *iot.QueryDevicePropertyStatusResponseBodyData, _err error) {
	request := &iot.QueryDevicePropertyStatusRequest{
		IotInstanceId:   pIotInstanceId,
		FunctionBlockId: pFunctionBlockId,
	}
	if !tea.BoolValue(util.Empty(pIotId)) {
		request.IotId = pIotId
	} else {
		request.ProductKey = pProductKey
		request.DeviceName = pDeviceName
	}

	response, _err := client.QueryDevicePropertyStatus(request)
	if _err != nil {
		return _result, _err
	}

	if !*response.Body.Success {
		return _result, fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}
	_result = response.Body.Data

	return _result, _err
}

func SetDeviceProperty(client *iot.Client, pIotInstanceId *string, pProductKey *string, pDeviceName *string, pIotId *string, pItems *string) (_err error) {
	request := &iot.SetDevicePropertyRequest{
		IotInstanceId: pIotInstanceId,
		ProductKey:    pProductKey,
		DeviceName:    pDeviceName,
		IotId:         pIotId,
		Items:         pItems,
	}
	console.Log(tea.String("-------------------4.为指定设备设置属性值:SetDeviceProperty--------------------"))
	console.Log(util.ToJSONString(tea.ToMap(request)))
	response, _err := client.SetDeviceProperty(request)
	if _err != nil {
		return _err
	}

	console.Log(util.ToJSONString(tea.ToMap(response.Body)))
	return _err
}
