package main

import (
	"testing"
)

func TestSelect(t *testing.T) {
	MysqlInit()

	data := CheckModeSelect(1)
	if data != nil {
		t.Log(data)
	}
	data = VoltageSelect(1)
	if data != nil {
		t.Log(data)
	}
	data = ErrorInfoSelect(1)
	if data != nil {
		t.Log(data)
	}

	result, err := CheckModeInsert(1)
	if err != nil {
		t.Log(err.Error())
	} else {
		num, _ := result.RowsAffected()
		id, _ := result.LastInsertId()
		t.Logf("num: %d, lastid: %d\n\r", num, id)
	}
	result, err = VoltageInsert("1.22")
	if err != nil {
		t.Log(err.Error())
	} else {
		num, _ := result.RowsAffected()
		id, _ := result.LastInsertId()
		t.Logf("num: %d, lastid: %d\n\r", num, id)
	}
	result, err = ErrorInfoInsert(1)
	if err != nil {
		t.Log(err.Error())
	} else {
		num, _ := result.RowsAffected()
		id, _ := result.LastInsertId()
		t.Logf("num: %d, lastid: %d\n\r", num, id)
	}

	data = CheckModeSelect(1)
	if data != nil {
		t.Log(data)
	}
	data = VoltageSelect(1)
	if data != nil {
		t.Log(data)
	}
	data = ErrorInfoSelect(1)
	if data != nil {
		t.Log(data)
	}
}
