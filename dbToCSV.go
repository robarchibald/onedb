package onedb

import (
	"encoding/csv"
	"fmt"
	"io"
	"time"
)

func writeCSV(rows RowsScanner, w io.Writer, options map[string]bool) error {
	headers, vals, err := getColumnNamesAndValues(rows, false)
	if err != nil {
		return err
	}

	csvWriter := csv.NewWriter(w)
	if err := csvWriter.Write(headers); err != nil {
		return err
	}
	for rows.Next() {
		row, err := scanCSV(rows, vals, options)
		if err != nil {
			return err
		}
		if err = csvWriter.Write(row); err != nil {
			return err
		}
	}
	csvWriter.Flush()
	return csvWriter.Error()
}

func scanCSV(s Scanner, vals []interface{}, options map[string]bool) ([]string, error) {
	err := s.Scan(vals...)
	if err != nil {
		return nil, err
	}

	row := make([]string, len(vals))
	for i, value := range vals {
		row[i] = getCSVValue(value.(*interface{}), options)
	}
	return row, nil
}

func getCSVValue(pval *interface{}, options map[string]bool) string {
	if options == nil {
		options = make(map[string]bool)
	}
	switch v := (*pval).(type) {
	case nil:
		return ""
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []byte:
		return encodeByteSlice(v, false)
	case time.Time:
		timeFormat := "2006-01-02 15:04:05.999"
		if options["dateOnly"] {
			timeFormat = "2006-01-02"
		}
		return v.Format(timeFormat)
	case string:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
