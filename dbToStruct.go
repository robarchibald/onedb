package onedb

import (
	"reflect"
	"strings"
	"time"

	"github.com/pkg/errors"
)

func getStruct(rows RowsScanner, result interface{}) error {
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

func getStructRow(rows RowsScanner, result interface{}) error {
	if rows.Err() != nil {
		return rows.Err()
	}
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

func scanStruct(s Scanner, vals []interface{}, dbToStruct map[int]structFieldInfo, result interface{}) error {
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

var timeKind = reflect.TypeOf(time.Time{}).Kind()
var nilType = reflect.TypeOf(nil)
var nilValue = reflect.ValueOf(nil)

// I have some values coming from the database that are nullable, and hence pointers to: bool, int16, string and time
// Unfortunately, I haven't figured out a better way to make it work than the lame if statement. Hopefully can replace
// with something better whenever I run across more nullable values to set.
func setValue(dest reflect.Value, src *interface{}) {
	if !dest.CanSet() {
		return
	}
	dest = getRootValue(dest)
	destType := dest.Type()
	destKind := destType.Kind()

	switch v := (*src).(type) {
	case nil:
	case bool:
		if destKind == reflect.Bool {
			dest.SetBool(v)
		}
	case []byte:
		if destKind == reflect.Slice && destType.Elem().Kind() == reflect.Uint8 {
			dest.SetBytes(v)
		}
	case float32:
		if destKind == reflect.Float32 || destKind == reflect.Float64 {
			dest.SetFloat(float64(v))
		}
	case float64:
		if destKind == reflect.Float64 {
			dest.SetFloat(v)
		}
	case int8:
		if destKind == reflect.Int8 || destKind == reflect.Int16 || destKind == reflect.Int32 || destKind == reflect.Int64 || destKind == reflect.Int {
			dest.SetInt(int64(v))
		}
	case int16:
		if destKind == reflect.Int16 || destKind == reflect.Int32 || destKind == reflect.Int64 || destKind == reflect.Int {
			dest.SetInt(int64(v))
		}
	case int32:
		if destKind == reflect.Int32 || destKind == reflect.Int64 || destKind == reflect.Int {
			dest.SetInt(int64(v))
		}
	case int64:
		if destKind == reflect.Int64 || destKind == reflect.Int {
			dest.SetInt(v)
		}
	case int:
		if destKind == reflect.Int || destKind == reflect.Int64 {
			dest.SetInt(int64(v))
		}
	case uint8:
		if destKind == reflect.Uint8 || destKind == reflect.Uint16 || destKind == reflect.Uint32 || destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(uint64(v))
		}
	case uint16:
		if destKind == reflect.Uint16 || destKind == reflect.Uint32 || destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(uint64(v))
		}
	case uint32:
		if destKind == reflect.Uint32 || destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(uint64(v))
		}
	case uint64:
		if destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(v)
		}
	case uint:
		if destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(uint64(v))
		}
	case string:
		if destType.Kind() == reflect.String {
			dest.SetString(v)
		}
	case time.Time:
		if destType.Kind() == timeKind {
			dest.Set(reflect.ValueOf(v))
		}
	default:
		dest.Set(reflect.ValueOf(v))
	}
}

func getRootValue(value reflect.Value) reflect.Value {
	if value.Kind() == reflect.Ptr {
		child := value.Elem()
		if child != nilValue {
			return getRootValue(child)
		}
	}
	return value
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
