package onedb

import (
	"encoding/csv"
	"fmt"
	"io"
	"time"
)

// CSVOptions contains specifications for how text should be formatted in a CSV file
type CSVOptions struct {
	DateOnly bool
}

func writeCSV(rows RowsScanner, w io.Writer, options CSVOptions) error {
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

func scanCSV(s Scanner, vals []interface{}, options CSVOptions) ([]string, error) {
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

func getCSVValue(pval *interface{}, options CSVOptions) string {
	switch v := (*pval).(type) {
	case nil:
		return ""
	case bool:
		if v {
			return "true"
		}
		return "false"
	case time.Time:
		timeFormat := "2006-01-02 15:04:05.999"
		if options.DateOnly {
			timeFormat = "2006-01-02"
		}
		return v.Format(timeFormat)
	case string:
		return string(v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
