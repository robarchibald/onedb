package onedb

import (
	"github.com/pkg/errors"
	"reflect"
	"strings"
	"time"
)

func getStruct(rows rowsScanner, result interface{}) error {
	columns, vals, err := getColumnNamesAndValues(rows, false)
	if err != nil {
		return err
	}

	itemType, dbToStruct := getItemTypeAndMap(columns, reflect.TypeOf(result).Elem())
	sliceValue := reflect.ValueOf(result).Elem()
	for rows.Next() {
		itemValue := reflect.New(itemType)
		err := scanStruct(rows, vals, dbToStruct, itemValue.Interface())
		if err != nil {
			return err
		}
		sliceValue.Set(reflect.Append(sliceValue, itemValue.Elem()))
	}
	return nil
}

func getStructRow(rows rowsScanner, result interface{}) error {
	if !rows.Next() {
		return errors.New("Empty result set")
	}
	columns, vals, err := getColumnNamesAndValues(rows, false)
	if err != nil {
		return err
	}

	_, dbToStruct := getItemTypeAndMap(columns, reflect.TypeOf(result))
	err = scanStruct(rows, vals, dbToStruct, result)
	if err != nil {
		return err
	}
	return nil
}

func scanStruct(s scanner, vals []interface{}, dbToStruct map[int]structFieldInfo, result interface{}) error {
	err := s.Scan(vals...)
	if err != nil {
		return err
	}
	item := reflect.ValueOf(result).Elem()
	for dbIndex, fieldInfo := range dbToStruct {
		setValue(item.Field(fieldInfo.Index), vals[dbIndex].(*interface{}))
	}
	return nil
}

// I have some values coming from the database that are nullable, and hence pointers to: bool, int16, string and time
// Unfortunately, I haven't figured out a better way to make it work than the lame if statement. Hopefully can replace
// with something better whenever I run across more nullable values to set.
func setValue(field reflect.Value, pval *interface{}) {
	if !field.CanSet() {
		return
	}
	fieldType := field.Type()
	dbType := reflect.TypeOf(*pval)
	if dbType != reflect.TypeOf(nil) && (fieldType != dbType && fieldType.Kind() != reflect.Ptr || fieldType.Kind() == reflect.Ptr && fieldType.Elem() != dbType) {
		return
	}
	switch v := (*pval).(type) {
	case nil:
	case bool:
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&v))
		} else {
			field.SetBool(v)
		}
	case []byte:
		field.SetBytes(v)
	case float32:
		field.SetFloat(float64(v))
	case float64:
		field.SetFloat(v)
	case int:
		field.SetInt(int64(v))
	case int8:
		field.SetInt(int64(v))
	case int16:
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&v))
		} else {
			field.SetInt(int64(v))
		}
	case int32:
		field.SetInt(int64(v))
	case int64:
		field.SetInt(v)
	case uint8:
		field.SetUint(uint64(v))
	case uint16:
		field.SetUint(uint64(v))
	case uint32:
		field.SetUint(uint64(v))
	case uint64:
		field.SetUint(v)
	case string:
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&v))
		} else {
			field.SetString(v)
		}
	case time.Time:
		if field.Kind() == reflect.Ptr {
			field.Set(reflect.ValueOf(&v))
		} else {
			field.Set(reflect.ValueOf(v))
		}
	default:
		field.Set(reflect.ValueOf(v))
	}
}

type structFieldInfo struct {
	Name  string
	Type  reflect.Type
	Index int
}

func getItemTypeAndMap(columns []string, resultType reflect.Type) (reflect.Type, map[int]structFieldInfo) {
	itemType := resultType.Elem()
	dbColumnToStruct := make(map[int]structFieldInfo)

	// make columns all lowercase
	for i, column := range columns {
		columns[i] = strings.ToLower(column)
	}

	for structIndex := 0; structIndex < itemType.NumField(); structIndex++ {
		field := itemType.Field(structIndex)
		structFieldInfo := structFieldInfo{strings.ToLower(field.Name), field.Type, structIndex}
		for dbIndex, column := range columns {
			if column == structFieldInfo.Name {
				dbColumnToStruct[dbIndex] = structFieldInfo
			}
		}
	}
	return itemType, dbColumnToStruct
}
