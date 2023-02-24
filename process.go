package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	"github.com/go-sql-driver/mysql"
	"github.com/thb-cmyk/aliyum-demo/utils"
)

var db *sql.DB

// define the statement of inserting and selecting for check_mode, voltage and error_info
var checkModeInsertStmt *sql.Stmt
var voltageInsertStmt *sql.Stmt
var errorInfoInsertStmt *sql.Stmt
var checkModeSelectStmt *sql.Stmt
var voltageSelectStmt *sql.Stmt
var errorInfoSelectStmt *sql.Stmt

// define the structure of check_mode, voltage and error_info
type VoltageStructure struct {
	Voltage string `json:"voltage"`
}

type CheckModeStructure struct {
	CheckMode int `json:"check_mode"`
}

type ErrorInfoStructure struct {
	ErrorInfo int `json:"error_info"`
}

/**
 * @brief: init the mysql database
 */
func MysqlInit() {

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
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		log.Panicf("Unable to open the database.\n\r error info: %s\n\r", err.Error())
	}

	// Call DB.Ping to confirm that connecting to the database works.
	err = db.Ping()
	if err != nil {
		log.Panicf("Unable to ping the database server. \n\r error info: %s\n\r", err.Error())
	}
	log.Printf("connected!\n\r")

	// intializing the statement of inserting and selecting for check_mode, voltage and error_info
	checkModeInsertStmt, err = db.Prepare("INSERT INTO check_mode (value) VALUES (?)")
	if err != nil {
		log.Panic("Unable to prepare the statement of insert to check_mode.\n\r", err.Error())
	}
	voltageInsertStmt, err = db.Prepare("INSERT INTO voltage (value) VALUES (?)")
	if err != nil {
		log.Panic("Unable to prepare the statement of insert to voltage. \n\r", err.Error())
	}
	errorInfoInsertStmt, err = db.Prepare("INSERT INTO error_info (value) VALUES (?)")
	if err != nil {
		log.Panic("Unable to prepare the statement of insert to error_info. \n\r", err.Error())
	}
	checkModeSelectStmt, err = db.Prepare("SELECT value FROM check_mode LIMIT ?")
	if err != nil {
		log.Panic("Unable to prepare the statement of select to check_mode. \n\r", err.Error())
	}
	voltageSelectStmt, err = db.Prepare("SELECT value FROM voltage LIMIT ?")
	if err != nil {
		log.Panic("Unable to prepare the statement of select to voltage. \n\r", err.Error())
	}
	errorInfoSelectStmt, err = db.Prepare("SELECT value FROM error_info LIMIT ?")
	if err != nil {
		log.Panic("Unable to prepare the statement of select to error_info. \n\r", err.Error())
	}

}

/**
 * @brief: deinit the mysql database
 */

func MysqlDeInit() {
	checkModeInsertStmt.Close()
	voltageInsertStmt.Close()
	errorInfoInsertStmt.Close()
	checkModeSelectStmt.Close()
	voltageSelectStmt.Close()
	errorInfoSelectStmt.Close()
	db.Close()
}

/**
 * @brief: insert the value to check_mode
 * @param: value
 * @return: sql.Result, error
 */
func CheckModeInsert(value int) (sql.Result, error) {
	result, err := checkModeInsertStmt.Exec(value)
	return result, err
}

/**
 * @brief: insert the value to voltage
 * @param: value
 * @return: sql.Result, error
 */
func VoltageInsert(value string) (sql.Result, error) {
	result, err := voltageInsertStmt.Exec(value)
	return result, err
}

/**
 * @brief: insert the value to error_info
 * @param: value
 * @return: sql.Result, error
 */
func ErrorInfoInsert(value int) (sql.Result, error) {
	result, err := errorInfoInsertStmt.Exec(value)
	return result, err
}

/**
 * @brief: select the value from check_mode
 * @param: index
 * @return: []byte
 */
func CheckModeSelect(index int) []byte {
	rows, _ := checkModeSelectStmt.Query(index)
	defer rows.Close()

	keys := make([]CheckModeStructure, 0)

	for rows.Next() {
		var key CheckModeStructure
		err := rows.Scan(&key.CheckMode)
		if err != nil {
			log.Print(err.Error())
			return nil
		}
		keys = append(keys, key)
	}

	keyStream, err := json.Marshal(keys)
	if err != nil {
		fmt.Print(err.Error())
	}
	return keyStream
}

/**
 * @brief: select the value from voltage
 * @param: index
 * @return: []byte
 */
func VoltageSelect(index int) []byte {
	rows, _ := voltageSelectStmt.Query(index)
	defer rows.Close()

	keys := make([]VoltageStructure, 0)

	for rows.Next() {
		var key VoltageStructure
		err := rows.Scan(&key.Voltage)
		if err != nil {
			log.Print(err.Error())
			return nil
		}
		keys = append(keys, key)
	}

	keyStream, err := json.Marshal(keys)
	if err != nil {
		fmt.Print(err.Error())
	}
	return keyStream
}

/**
 * @brief: select the value from error_info
 * @param: index
 * @return: []byte
 */
func ErrorInfoSelect(index int) []byte {
	rows, _ := errorInfoSelectStmt.Query(index)
	defer rows.Close()

	keys := make([]ErrorInfoStructure, 0)

	for rows.Next() {
		var key ErrorInfoStructure
		err := rows.Scan(&key.ErrorInfo)
		if err != nil {
			log.Print(err.Error())
			return nil
		}
		keys = append(keys, key)
	}

	keyStream, err := json.Marshal(keys)
	if err != nil {
		fmt.Print(err.Error())
	}
	return keyStream
}
