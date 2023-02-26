package main

import (
	"container/list"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-sql-driver/mysql"
	"github.com/thb-cmyk/aliyum-demo/utils"
)

var db *sql.DB

var tables *list.List

// define the structure of check_mode, voltage and error_info
type VoltageStructure struct {
	Voltage    float64 `json:"voltage"`
	DeviceName string  `json:"device_name"`
	Time       string  `json:"time"`
}

type CheckModeStructure struct {
	CheckMode  int    `json:"check_mode"`
	DeviceName string `json:"device_name"`
	Time       string `json:"time"`
}

type ErrorInfoStructure struct {
	ErrorInfo  int    `json:"error_info"`
	DeviceName string `json:"device_name"`
	Time       string `json:"time"`
}

type StatusStructure struct {
	Status     string `json:"status"`
	DeviceName string `json:"device_name"`
	Time       string `json:"time"`
}

type GeneralStructure struct {
	DeviceName string      `json:"device_name"`
	Time       string      `json:"time"`
	Value      interface{} `json:"value"`
}

type ValueStructure struct {
	Params map[string]interface{} `json:"params"`
}

/**
 * @brief: init the mysql database
 */
func MysqlInit() {
	var err error

	// get the configuration from config.yaml
	configMap := utils.GetYamlConfig("config/config.yaml")
	user := configMap["User"].(string)
	passwd := configMap["Passwd"].(string)
	net := configMap["Net"].(string)
	addr := configMap["Addr"].(string)
	dbName := configMap["DBName"].(string)
	allowNativePasswords := configMap["AllowNativePasswords"].(bool)

	// Capture connection properties.
	cfg := mysql.Config{
		User:                 user,
		Passwd:               passwd,
		Net:                  net,
		Addr:                 addr,
		DBName:               dbName,
		AllowNativePasswords: allowNativePasswords,
	}

	// open connection to the database
	db, err = sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Panicf("Unable to open the database.\n\r error info: %s\n\r", err.Error())
	}

	// Call DB.Ping to confirm that connecting to the database works.
	err = db.Ping()
	if err != nil {
		log.Panicf("Unable to ping the database server. \n\r error info: %s\n\r", err.Error())
	}
	log.Printf("connected!\n\r")

	// init tables
	tables = list.New()

	// get the tables list from the remote database server
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		log.Printf("Unable to get the tables list. \n\r error info: %s\n\r", err.Error())
	}
	defer rows.Close()
	for rows.Next() {
		var table_name string
		rows.Scan(&table_name)
		tables.PushBack(table_name)
	}
	for e := tables.Front(); e != nil; e = e.Next() {
		fmt.Println(e.Value)
	}
}

/**
 * @brief: deinit the mysql database
 */

func MysqlDeInit() {

	// deinit tables
	tables = nil

	db.Close()
}

func Insert(gt GeneralStructure) (sql.Result, error) {
	value := gt.Value.(ValueStructure).Params
	deviceName := gt.DeviceName
	for key, value := range value {
		table_name := deviceName + key
		fmt.Print(table_name)
		switch key {
		case "voltage":
			voltage_value := value.(float64)
			var voltage VoltageStructure
			voltage.DeviceName = deviceName
			voltage.Voltage = voltage_value
			voltage.Time = gt.Time
			// find the table in the tables list
			for e := tables.Front(); e != nil; e = e.Next() {
				if e.Value == table_name {
					// insert the value to the table
					result, err := VoltageInsertStmt(voltage)
					return result, err

				}
			}
			// the table of named talbe_name is not exist
			// create the table
			err := CreateTable(deviceName, "voltage")
			if err != nil {
				log.Fatal("Unable to create the table of voltage. \n\r", err.Error())
				return nil, err
			}
			// insert the value to the table
			result, err := VoltageInsertStmt(voltage)
			return result, err

		case "check_mode":
			check_mode_value := int(value.(float64))
			var check_mode CheckModeStructure
			check_mode.DeviceName = deviceName
			check_mode.CheckMode = check_mode_value
			check_mode.Time = gt.Time
			// find the table in the tables list
			for e := tables.Front(); e != nil; e = e.Next() {
				if e.Value == table_name {
					// insert the value to the table
					result, err := CheckModeInsertStmt(check_mode)
					return result, err
				}
			}
			// the table of named talbe_name is not exist
			// create the table
			err := CreateTable(deviceName, "check_mode")
			if err != nil {
				log.Fatal("Unable to create the table of check_mode. \n\r", err.Error())
				return nil, err
			}
			// insert the value to the table
			result, err := CheckModeInsertStmt(check_mode)
			return result, err

		case "error_info":
			error_info_value := int(value.(float64))
			var error_info ErrorInfoStructure
			error_info.DeviceName = deviceName
			error_info.ErrorInfo = error_info_value
			error_info.Time = gt.Time
			// find the table in the tables list
			for e := tables.Front(); e != nil; e = e.Next() {
				if e.Value == table_name {
					// insert the value to the table
					result, err := ErrorInfoInsertStmt(error_info)
					return result, err
				}
			}
			// the table of named talbe_name is not exist
			// create the table
			err := CreateTable(deviceName, "error_info")
			if err != nil {
				log.Fatal("Unable to create the table of error_info. \n\r", err.Error())
				return nil, err
			}
			// insert the value to the table
			result, err := ErrorInfoInsertStmt(error_info)
			return result, err

		case "status":
			status_value := value.(string)
			var status StatusStructure
			status.DeviceName = deviceName
			status.Status = status_value
			status.Time = gt.Time
			// find the table in the tables list
			for e := tables.Front(); e != nil; e = e.Next() {
				if e.Value == table_name {
					// insert the value to the table
					result, err := StatusInsertStmt(status)
					return result, err
				}
			}
			// the table of named talbe_name is not exist
			// create the table
			err := CreateTable(deviceName, "status")
			if err != nil {
				log.Fatal("Unable to create the table of status. \n\r", err.Error())
				return nil, err
			}
			// insert the value to the table
			result, err := StatusInsertStmt(status)
			return result, err

		default:
			log.Printf("The key is not in the table.\n\r")
			return nil, nil
		}
	}
	return nil, nil

}

func Select(deviceName string, table_type string, index int) []byte {
	switch table_type {
	case "voltage":
		tableName := deviceName + table_type
		for e := tables.Front(); e != nil; e = e.Next() {
			if e.Value == tableName {
				data := VoltageSelectStmt(deviceName, index)
				return data
			}
		}
		data := []byte("The table is not exist.")
		return data
	case "check_mode":
		tableName := deviceName + table_type
		for e := tables.Front(); e != nil; e = e.Next() {
			if e.Value == tableName {
				data := CheckModeSelectStmt(deviceName, index)
				return data
			}
		}
		data := []byte("The table is not exist.")
		return data
	case "error_info":
		tableName := deviceName + table_type
		for e := tables.Front(); e != nil; e = e.Next() {
			if e.Value == tableName {
				data := ErrorInfoSelectStmt(deviceName, index)
				return data
			}
		}
		data := []byte("The table is not exist.")
		return data
	case "status":
		tableName := deviceName + table_type
		for e := tables.Front(); e != nil; e = e.Next() {
			if e.Value == tableName {
				data := StatusSelectStmt(deviceName, index)
				return data
			}
		}
		data := []byte("The table is not exist.")
		return data

	default:
	}
	data := []byte("The requst is error.")
	return data
}

func CreateTable(deviceName string, table_type string) error {
	switch table_type {
	case "voltage":
		err := VoltageCreateStmt(deviceName)
		if err != nil {
			log.Printf("Unable to create the table of voltage.\n\r")
			return err
		}

	case "check_mode":
		err := CheckModeCreateStmt(deviceName)
		if err != nil {
			log.Printf("Unable to create the table of check_mode.\n\r")
			return err
		}

	case "error_info":
		err := ErrorInfoCreateStmt(deviceName)
		if err != nil {
			log.Printf("Unable to create the table of error_info.\n\r")
			return err
		}
	case "status":
		err := StatusCreateStmt(deviceName)
		if err != nil {
			log.Printf("Unable to create the table of status.\n\r")
			return err
		}

	default:
		log.Printf("The table type is not in the table.\n\r")

	}
	return nil

}

func CheckModeCreateStmt(deviceName string) error {

	stmt_string := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT NOT NULL AUTO_INCREMENT, value INT, device_name VARCHAR(50), time VARCHAR(50), PRIMARY KEY (id))", deviceName+"check_mode")
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of create check_mode table.\n\r", err.Error())
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		log.Panic("Unable to create the table of check_mode.\n\r", err.Error())
		return err
	}
	tables.PushFront(deviceName + "check_mode")
	return nil
}

func VoltageCreateStmt(deviceName string) error {
	stmt_string := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT NOT NULL AUTO_INCREMENT, value FLOAT, device_name VARCHAR(50), time VARCHAR(50), PRIMARY KEY (id))", deviceName+"voltage")
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Printf("Unable to create the statement of create voltage table.\n\r", err.Error())
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		log.Printf("Unable to create the table of voltage.\n\r", err.Error())
		return err
	}
	tables.PushFront(deviceName + "voltage")
	return nil
}

func ErrorInfoCreateStmt(deviceName string) error {
	stmt_string := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT NOT NULL AUTO_INCREMENT, value INT, device_name VARCHAR(50), time VARCHAR(50), PRIMARY KEY (id))", deviceName+"error_info")
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of create error_info table.\n\r", err.Error())
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		log.Panic("Unable to create the table of error_info.\n\r", err.Error())
		return err
	}
	tables.PushFront(deviceName + "voltage")
	return nil
}

func StatusCreateStmt(deviceName string) error {
	stmt_string := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (id INT NOT NULL AUTO_INCREMENT, value VARCHAR(10), device_name VARCHAR(50), time VARCHAR(50), PRIMARY KEY (id))", deviceName+"status")
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of create status table.\n\r", err.Error())
		return err
	}
	defer stmt.Close()
	_, err = stmt.Exec()
	if err != nil {
		log.Panic("Unable to create the table of status.\n\r", err.Error())
		return err
	}
	tables.PushFront(deviceName + "status")
	return nil
}

func CheckModeInsertStmt(checkMode CheckModeStructure) (sql.Result, error) {

	deviceName := checkMode.DeviceName
	table_name := deviceName + "check_mode"
	time := checkMode.Time
	value := checkMode.CheckMode

	stmt_string := fmt.Sprintf("INSERT INTO %s (value, device_name, time) VALUES (%d, \"%s\", \"%s\")", table_name, value, deviceName, time)
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of insert to check_mode.\n\r", err.Error())
		return nil, err
	}
	defer stmt.Close()
	result, err := stmt.Exec()
	if err != nil {
		log.Panic("Unable to insert to check_mode.\n\r", err.Error())
		return nil, err
	}
	log.Println("Insert to check_mode successfully. The result is ", result)
	return result, nil
}

func VoltageInsertStmt(voltage VoltageStructure) (sql.Result, error) {
	deviceName := voltage.DeviceName
	time := voltage.Time
	value := voltage.Voltage
	table_name := deviceName + "voltage"

	stmt_string := fmt.Sprintf("INSERT INTO %s (value, device_name, time) VALUES (%f, \"%s\", \"%s\")", table_name, value, deviceName, time)
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of insert to voltage. \n\r", err.Error())
		return nil, err
	}
	defer stmt.Close()
	result, err := stmt.Exec()
	if err != nil {
		log.Panic("Unable to insert to voltage.\n\r", err.Error())
		return nil, err
	}
	log.Println("Insert to voltage successfully. The result is ", result)
	return result, nil
}

func ErrorInfoInsertStmt(errorInfo ErrorInfoStructure) (sql.Result, error) {
	deviceName := errorInfo.DeviceName
	time := errorInfo.Time
	value := errorInfo.ErrorInfo
	table_name := deviceName + "error_info"

	stmt_string := fmt.Sprintf("INSERT INTO %s (value, device_name, time) VALUES (%d, \"%s\", \"%s\")", table_name, value, deviceName, time)
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Printf("Unable to create the statement of insert to error_info. \n\r", err.Error())
		return nil, err
	}
	defer stmt.Close()
	result, err := stmt.Exec()
	if err != nil {
		log.Printf("Unable to insert to error_info.\n\r", err.Error())
		return nil, err
	}
	log.Println("Insert to error_info successfully. The result is ", result)
	return result, nil
}

func StatusInsertStmt(status StatusStructure) (sql.Result, error) {
	deviceName := status.DeviceName
	time := status.Time
	value := status.Status
	table_name := deviceName + "status"

	stmt_string := fmt.Sprintf("INSERT INTO %s (value, device_name, time) VALUES (\"%s\", \"%s\", \"%s\")", table_name, value, deviceName, time)
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of insert to status.\n\r", err.Error())
		return nil, err
	}
	defer stmt.Close()
	result, err := stmt.Exec()
	if err != nil {
		log.Panic("Unable to insert to status.\n\r", err.Error())
		return nil, err
	}
	log.Println("Insert to status successfully. The result is ", result)
	return result, nil
}

func CheckModeSelectStmt(deviceName string, index int) []byte {

	stmt_string := fmt.Sprintf("SELECT value, device_name, time FROM %s ORDER BY id DESC LIMIT %d", deviceName+"check_mode", index)
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of select to check_mode. \n\r", err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Panic("Unable to select to check_mode.\n\r", err.Error())
	}
	defer rows.Close()
	var checkModeStructures []CheckModeStructure
	for rows.Next() {
		var checkModeStructure CheckModeStructure
		err := rows.Scan(&checkModeStructure.CheckMode, &checkModeStructure.DeviceName, &checkModeStructure.Time)
		if err != nil {
			log.Panic("Unable to scan the result of select to check_mode.\n\r", err.Error())
		}
		checkModeStructures = append(checkModeStructures, checkModeStructure)
	}
	keyStream, err := json.Marshal(checkModeStructures)
	if err != nil {
		fmt.Print(err.Error())
	}
	return keyStream

}

func VoltageSelectStmt(deviceName string, index int) []byte {

	stmt_string := fmt.Sprintf("SELECT value, device_name, time FROM %s ORDER BY id DESC LIMIT %d", deviceName+"voltage", index)
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of select to voltage. \n\r", err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Panic("Unable to select to voltage.\n\r", err.Error())
	}
	defer rows.Close()
	var voltageStructures []VoltageStructure
	for rows.Next() {
		var voltageStructure VoltageStructure
		err := rows.Scan(&voltageStructure.Voltage, &voltageStructure.DeviceName, &voltageStructure.Time)
		if err != nil {
			log.Panic("Unable to scan the result of select to voltage.\n\r", err.Error())
		}
		voltageStructures = append(voltageStructures, voltageStructure)
	}
	keyStream, err := json.Marshal(voltageStructures)
	if err != nil {
		fmt.Print(err.Error())
	}
	return keyStream
}

func ErrorInfoSelectStmt(deviceName string, index int) []byte {

	stmt_string := fmt.Sprintf("SELECT value, device_name, time FROM %s ORDER BY id DESC LIMIT %d", deviceName+"error_info", index)
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of select to error_info. \n\r", err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {

		log.Panic("Unable to select to error_info.\n\r", err.Error())
	}
	defer rows.Close()
	var errorInfoStructures []ErrorInfoStructure
	for rows.Next() {
		var errorInfoStructure ErrorInfoStructure

		err := rows.Scan(&errorInfoStructure.ErrorInfo, &errorInfoStructure.DeviceName, &errorInfoStructure.Time)
		if err != nil {
			log.Panic("Unable to scan the result of select to error_info.\n\r", err.Error())
		}
		errorInfoStructures = append(errorInfoStructures, errorInfoStructure)
	}
	keyStream, err := json.Marshal(errorInfoStructures)
	if err != nil {
		fmt.Print(err.Error())
	}
	return keyStream
}

func StatusSelectStmt(deviceName string, index int) []byte {

	stmt_string := fmt.Sprintf("SELECT value, device_name, time FROM %s ORDER BY id DESC LIMIT %d", deviceName+"status", index)
	stmt, err := db.Prepare(stmt_string)
	if err != nil {
		log.Panic("Unable to create the statement of select to status. \n\r", err.Error())
	}
	defer stmt.Close()
	rows, err := stmt.Query()
	if err != nil {
		log.Panic("Unable to select to status.\n\r", err.Error())
	}
	defer rows.Close()
	var statusStructures []StatusStructure
	for rows.Next() {
		var statusStructure StatusStructure
		err := rows.Scan(&statusStructure.Status, &statusStructure.DeviceName, &statusStructure.Time)
		if err != nil {
			log.Panic("Unable to scan the result of select to status.\n\r", err.Error())
		}
		statusStructures = append(statusStructures, statusStructure)
	}
	keyStream, err := json.Marshal(statusStructures)
	if err != nil {
		fmt.Print(err.Error())
	}
	return keyStream
}
