package main

import (
	"fmt"

	iot "github.com/alibabacloud-go/iot-20180120/v3/client"
)

/**
 * 为指定产品的物模型新增功能，支持同时新增物模型扩展描述
 *
 * @param iotInstanceId
 * @param productKey
 * @param thingModelJson
 * @param functionBlockId
 * @param functionBlockName
 */
func CreateThingModel(client *iot.Client, iotInstanceId *string, productKey *string, thingModelJson *string, functionBlockId *string, functionBlockName *string) (_err error) {
	request := &iot.CreateThingModelRequest{
		IotInstanceId:     iotInstanceId,
		ProductKey:        productKey,
		ThingModelJson:    thingModelJson,
		FunctionBlockId:   functionBlockId,
		FunctionBlockName: functionBlockName,
	}

	response, _err := client.CreateThingModel(request)
	if _err != nil {
		return _err
	}
	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err
}

/**
 * 查看指定产品的物模型中的功能定义详情
 *
 * @param iotInstanceId
 * @param productKey
 * @param modelVersion
 */
func QueryThingModel(client *iot.Client, iotInstanceId *string, productKey *string, modelVersion *string) (_result *iot.QueryThingModelResponseBodyData, _err error) {
	request := &iot.QueryThingModelRequest{
		IotInstanceId: iotInstanceId,
		ProductKey:    productKey,
		ModelVersion:  modelVersion,
	}
	response, _err := client.QueryThingModel(request)
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
 * 查看指定产品的已发布物模型中的功能定义详情
 *
 * @param iotInstanceId
 * @param productKey
 * @param thingModelJson
 * @param functionBlockId
 */
func QueryThingModelPublished(client *iot.Client, iotInstanceId *string, productKey *string, modelVersion *string, functionBlockId *string) (_result *iot.QueryThingModelPublishedResponseBodyData, _err error) {
	request := &iot.QueryThingModelPublishedRequest{
		IotInstanceId:   iotInstanceId,
		ProductKey:      productKey,
		ModelVersion:    modelVersion,
		FunctionBlockId: functionBlockId,
	}
	response, _err := client.QueryThingModelPublished(request)
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
 * 查询指定产品的物模型TSL
 *
 * @param iotInstanceId
 * @param productKey
 * @param modelVersion
 * @param functionBlockId
 * @param simple
 */
func GetThingModelTsl(client *iot.Client, iotInstanceId *string, productKey *string, modelVersion *string, functionBlockId *string, simple *bool) (_result *iot.GetThingModelTslResponseBodyData, _err error) {
	request := &iot.GetThingModelTslRequest{
		IotInstanceId:   iotInstanceId,
		ProductKey:      productKey,
		ModelVersion:    modelVersion,
		FunctionBlockId: functionBlockId,
		Simple:          simple,
	}
	response, _err := client.GetThingModelTsl(request)
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
 * 查询指定产品的已发布物模型TSL
 *
 * @param iotInstanceId
 * @param productKey
 * @param modelVersion
 * @param functionBlockId
 * @param simple
 */
func GetThingModelTslPublished(client *iot.Client, iotInstanceId *string, productKey *string, modelVersion *string, functionBlockId *string, simple *bool) (_result *iot.GetThingModelTslPublishedResponseBodyData, _err error) {
	request := &iot.GetThingModelTslPublishedRequest{
		IotInstanceId:   iotInstanceId,
		ProductKey:      productKey,
		ModelVersion:    modelVersion,
		FunctionBlockId: functionBlockId,
		Simple:          simple,
	}
	response, _err := client.GetThingModelTslPublished(request)
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
 * 获取指定产品的物模型版本列表
 *
 * @param iotInstanceId
 * @param productKey
 */
func ListThingModelVersion(client *iot.Client, iotInstanceId *string, productKey *string) (_result *iot.ListThingModelVersionResponseBodyData, _err error) {
	request := &iot.ListThingModelVersionRequest{
		IotInstanceId: iotInstanceId,
		ProductKey:    productKey,
	}
	response, _err := client.ListThingModelVersion(request)
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
 * 更新指定产品物模型中的单个功能，支持更新物模型扩展描述
 *
 * @param iotInstanceId
 * @param productKey
 * @param thingModelJson
 * @param functionBlockId
 * @param functionBlockName
 * @param identifier
 */
func UpdateThingModel(client *iot.Client, iotInstanceId *string, productKey *string, thingModelJson *string, functionBlockId *string, functionBlockName *string, identifier *string) (_err error) {
	request := &iot.UpdateThingModelRequest{
		IotInstanceId:     iotInstanceId,
		ProductKey:        productKey,
		ThingModelJson:    thingModelJson,
		FunctionBlockId:   functionBlockId,
		FunctionBlockName: functionBlockName,
		Identifier:        identifier,
	}

	response, _err := client.UpdateThingModel(request)
	if _err != nil {
		return _err
	}

	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err
}

/**
 * 复制指定产品的物模型到目标产品
 *
 * @param iotInstanceId
 * @param sourceProductKey
 * @param targetProductKey
 * @param sourceModelVersion
 */
func CopyThingModel(client *iot.Client, iotInstanceId *string, sourceProductKey *string, targetProductKey *string, sourceModelVersion *string) (_err error) {
	request := &iot.CopyThingModelRequest{
		IotInstanceId:      iotInstanceId,
		SourceProductKey:   sourceProductKey,
		TargetProductKey:   targetProductKey,
		SourceModelVersion: sourceModelVersion,
	}

	response, _err := client.CopyThingModel(request)
	if _err != nil {
		return _err
	}

	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err
}

/**
 * 为指定产品导入物模型
 *
 * @param iotInstanceId
 * @param productKey
 * @param tslStr
 * @param functionBlockId
 * @param functionBlockName
 */
func ImportThingModelTsl(client *iot.Client, iotInstanceId *string, productKey *string, tslStr *string, functionBlockId *string, functionBlockName *string) (_err error) {
	request := &iot.ImportThingModelTslRequest{
		IotInstanceId:     iotInstanceId,
		ProductKey:        productKey,
		TslStr:            tslStr,
		FunctionBlockId:   functionBlockId,
		FunctionBlockName: functionBlockName,
	}

	response, _err := client.ImportThingModelTsl(request)
	if _err != nil {
		return _err
	}

	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err
}

/**
 * 发布指定产品的物模型
 *
 * @param iotInstanceId
 * @param productKey
 * @param modelVersion
 * @param description
 */
func PublishThingModel(client *iot.Client, iotInstanceId *string, productKey *string, modelVersion *string, description *string) (_err error) {
	request := &iot.PublishThingModelRequest{
		IotInstanceId: iotInstanceId,
		ProductKey:    productKey,
		ModelVersion:  modelVersion,
		Description:   description,
	}

	response, _err := client.PublishThingModel(request)
	if _err != nil {
		return _err
	}

	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err
}

/**
 * 查询物模型扩展描述配置
 *
 * @param iotInstanceId
 * @param productKey
 * @param modelVersion
 * @param functionBlockId
 */
func QueryThingModelExtendConfig(client *iot.Client, iotInstanceId *string, productKey *string, modelVersion *string, functionBlockId *string) (_result *iot.QueryThingModelExtendConfigResponseBodyData, _err error) {
	request := &iot.QueryThingModelExtendConfigRequest{
		IotInstanceId:   iotInstanceId,
		ProductKey:      productKey,
		ModelVersion:    modelVersion,
		FunctionBlockId: functionBlockId,
	}

	response, _err := client.QueryThingModelExtendConfig(request)
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
 * 获取已发布物模型的扩展描述配置
 *
 * @param iotInstanceId
 * @param productKey
 * @param modelVersion
 */
func QueryThingModelExtendConfigPublished(client *iot.Client, iotInstanceId *string, productKey *string, modelVersion *string) (_result *iot.QueryThingModelExtendConfigPublishedResponseBodyData, _err error) {
	request := &iot.QueryThingModelExtendConfigPublishedRequest{
		IotInstanceId: iotInstanceId,
		ProductKey:    productKey,
		ModelVersion:  modelVersion,
	}

	response, _err := client.QueryThingModelExtendConfigPublished(request)
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
 * 为指定产品的物模型新增功能，支持同时新增物模型扩展描述
 *
 * @param iotInstanceId
 * @param productKey
 * @param thingModelJson
 * @param functionBlockId
 * @param functionBlockName
 */
func DeleteThingModel(client *iot.Client, iotInstanceId *string, productKey *string, propertyIdentifier []*string, serviceIdentifier []*string, eventIdentifier []*string, functionBlockId *string) (_err error) {
	request := &iot.DeleteThingModelRequest{
		IotInstanceId:      iotInstanceId,
		ProductKey:         productKey,
		PropertyIdentifier: propertyIdentifier,
		ServiceIdentifier:  serviceIdentifier,
		EventIdentifier:    eventIdentifier,
		FunctionBlockId:    functionBlockId,
	}

	response, _err := client.DeleteThingModel(request)
	if _err != nil {
		return _err
	}

	if !*response.Body.Success {
		return fmt.Errorf("code: %s, error message: %s", *response.Body.Code, *response.Body.ErrorMessage)
	}

	return _err
}
