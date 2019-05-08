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

func scanStruct(s Scanner, vals []interface{}, dbToStruct []structFieldInfo, result interface{}) error {
	err := s.Scan(vals...)
	if err != nil {
		return err
	}
	item := reflect.ValueOf(result).Elem()
	for _, fieldInfo := range dbToStruct {
		setValue(item.Field(fieldInfo.FieldIndex), vals[fieldInfo.DBIndex].(*interface{}))
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
	destType := dest.Type()
	destKind := destType.Kind()
	destRootKind := destKind
	if destKind == reflect.Ptr {
		destRootKind = destType.Elem().Kind()
	}

	switch v := (*src).(type) {
	case nil:
	case bool:
		if destKind == reflect.Bool {
			dest.SetBool(v)
		} else if destKind == reflect.Ptr && destRootKind == reflect.Bool {
			dest.Set(reflect.ValueOf(&v))
		}
	case []byte:
		if destKind == reflect.Slice && destType.Elem().Kind() == reflect.Uint8 {
			dest.SetBytes(v)
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Slice && destType.Elem().Elem().Kind() == reflect.Uint8) {
			dest.Set(reflect.ValueOf(&v))
		}
	case float32:
		if destKind == reflect.Float32 || destKind == reflect.Float64 {
			dest.SetFloat(float64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Float32 || destRootKind == reflect.Float64) {
			dest.Set(reflect.ValueOf(&v))
		}
	case float64:
		if destKind == reflect.Float64 {
			dest.SetFloat(v)
		} else if destKind == reflect.Ptr && destRootKind == reflect.Float64 {
			dest.Set(reflect.ValueOf(&v))
		}
	case int8:
		if destKind == reflect.Int8 || destKind == reflect.Int16 || destKind == reflect.Int32 || destKind == reflect.Int64 || destKind == reflect.Int {
			dest.SetInt(int64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Int8 || destRootKind == reflect.Int16 || destRootKind == reflect.Int32 || destRootKind == reflect.Int64 || destRootKind == reflect.Int) {
			dest.Set(reflect.ValueOf(&v))
		}
	case int16:
		if destKind == reflect.Int16 || destKind == reflect.Int32 || destKind == reflect.Int64 || destKind == reflect.Int {
			dest.SetInt(int64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Int16 || destRootKind == reflect.Int32 || destRootKind == reflect.Int64 || destRootKind == reflect.Int) {
			dest.Set(reflect.ValueOf(&v))
		}
	case int32:
		if destKind == reflect.Int32 || destKind == reflect.Int64 || destKind == reflect.Int {
			dest.SetInt(int64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Int32 || destRootKind == reflect.Int64 || destRootKind == reflect.Int) {
			dest.Set(reflect.ValueOf(&v))
		}
	case int64:
		if destKind == reflect.Int64 || destKind == reflect.Int {
			dest.SetInt(v)
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Int64 || destRootKind == reflect.Int) {
			dest.Set(reflect.ValueOf(&v))
		}
	case int:
		if destKind == reflect.Int || destKind == reflect.Int64 {
			dest.SetInt(int64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Int64 || destRootKind == reflect.Int) {
			dest.Set(reflect.ValueOf(&v))
		}
	case uint8:
		if destKind == reflect.Uint8 || destKind == reflect.Uint16 || destKind == reflect.Uint32 || destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(uint64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Uint8 || destRootKind == reflect.Uint16 || destRootKind == reflect.Uint32 || destRootKind == reflect.Uint64 || destRootKind == reflect.Uint) {
			dest.Set(reflect.ValueOf(&v))
		}
	case uint16:
		if destKind == reflect.Uint16 || destKind == reflect.Uint32 || destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(uint64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Uint16 || destRootKind == reflect.Uint32 || destRootKind == reflect.Uint64 || destRootKind == reflect.Uint) {
			dest.Set(reflect.ValueOf(&v))
		}
	case uint32:
		if destKind == reflect.Uint32 || destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(uint64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Uint32 || destRootKind == reflect.Uint64 || destRootKind == reflect.Uint) {
			dest.Set(reflect.ValueOf(&v))
		}
	case uint64:
		if destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(v)
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Uint64 || destRootKind == reflect.Uint) {
			dest.Set(reflect.ValueOf(&v))
		}
	case uint:
		if destKind == reflect.Uint64 || destKind == reflect.Uint {
			dest.SetUint(uint64(v))
		} else if destKind == reflect.Ptr && (destRootKind == reflect.Uint64 || destRootKind == reflect.Uint) {
			dest.Set(reflect.ValueOf(&v))
		}
	case string:
		if destKind == reflect.String {
			dest.SetString(v)
		} else if destKind == reflect.Ptr && destRootKind == reflect.String {
			dest.Set(reflect.ValueOf(&v))
		}
	case time.Time:
		if destKind == timeKind {
			dest.Set(reflect.ValueOf(v))
		} else if destKind == reflect.Ptr && destRootKind == timeKind {
			dest.Set(reflect.ValueOf(&v))
		}
	default:
		if destType != reflect.TypeOf(*src) {
			return
		}
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
	Name       string
	Type       reflect.Type
	FieldIndex int
	DBIndex    int
}

func getItemTypeAndMap(columns []string, resultType reflect.Type) (reflect.Type, []structFieldInfo) {
	itemType := resultType.Elem()
	dbColumnToStruct := []structFieldInfo{}

	// make columns all lowercase
	for i, column := range columns {
		columns[i] = strings.ToLower(column)
	}

	for structIndex := 0; structIndex < itemType.NumField(); structIndex++ {
		field := itemType.Field(structIndex)
		if dbIndex := getDBIndex(field.Name, columns); dbIndex != -1 {
			dbColumnToStruct = append(dbColumnToStruct, structFieldInfo{strings.ToLower(field.Name), field.Type, structIndex, dbIndex})
		}
	}
	return itemType, dbColumnToStruct
}

func getDBIndex(name string, columns []string) int {
	for i, column := range columns {
		if column == strings.ToLower(name) {
			return i
		}
	}
	return -1
}
