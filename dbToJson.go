package onedb

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"time"
	"unicode/utf8"
)

func getJSON(rows RowsScanner) (string, error) {
	columns, vals, err := getColumnNamesAndValues(rows, true)
	if err != nil {
		return "", err
	}

	var b bytes.Buffer
	writeComma := false
	b.WriteByte('[')
	for rows.Next() {
		err := scanJSON(rows, columns, vals, writeComma, &b)
		if err != nil {
			return "", err
		}
		writeComma = true
	}
	b.WriteByte(']')

	return b.String(), nil
}

func getJSONRow(rows RowsScanner) (string, error) {
	columns, vals, err := getColumnNamesAndValues(rows, true)
	if err != nil {
		return "", err
	}

	var b bytes.Buffer
	if rows.Next() {
		err := scanJSON(rows, columns, vals, false, &b)
		if err != nil {
			return "", err
		}
	}

	return b.String(), nil
}

func scanJSON(s Scanner, columns []string, vals []interface{}, writeComma bool, b *bytes.Buffer) error {
	if writeComma {
		b.WriteByte(',')
	}
	b.WriteByte('{')
	err := s.Scan(vals...)
	if err != nil {
		return err
	}
	firstColumn := true
	for i := 0; i < len(vals); i++ {
		jsonValue := getJSONValue(vals[i].(*interface{}))
		if jsonValue != "null" {
			if !firstColumn {
				b.WriteByte(',')
			}
			b.WriteString(columns[i])
			b.WriteString(jsonValue)
			firstColumn = false
		}
	}
	b.WriteByte('}')
	return nil
}

func getColumnNamesAndValues(s RowsScanner, isJSON bool) ([]string, []interface{}, error) {
	if s.Err() != nil {
		return nil, nil, s.Err()
	}

	columnNames, err := s.Columns()
	if err != nil {
		return nil, nil, err
	}

	values := make([]interface{}, len(columnNames))
	for i := 0; i < len(columnNames); i++ {
		values[i] = new(interface{})

		if isJSON {
			columnNames[i] = jsonize(columnNames[i])
		}
	}
	return columnNames, values, nil
}

func jsonize(columnName string) string {
	var buffer bytes.Buffer
	buffer.WriteByte('"')
	buffer.WriteString(columnName)
	buffer.WriteByte('"')
	buffer.WriteByte(':')
	return buffer.String()
}

func getJSONValue(pval *interface{}) string {
	switch v := (*pval).(type) {
	case nil:
		return "null"
	case bool:
		if v {
			return "true"
		}
		return "false"
	case []byte:
		return encodeByteSlice(v, true)
	case time.Time:
		return v.Format(`"2006-01-02 15:04:05.999"`)
	case uint8, uint16, uint32, uint64, int, int8, int16, int32, int64, float32, float64, complex64, complex128:
		return fmt.Sprintf("%v", v) // probably not optimized for speed since Sprintf is relatively slow
	case string:
		return encodeString(string(v))
	default:
		return encodeString(fmt.Sprintf("%v", v)) // probably not optimized for speed since Sprintf is relatively slow
	}
}

// these methods are taken directly from the "encoding/json" library and modified to return a string
// and use a simple bytes.Buffer instead of its original encodeState struct which is a light wrapper
// over the bytes.Buffer
var hex = "0123456789abcdef"

func encodeByteSlice(byteSlice []byte, useQuotes bool) string {
	if len(byteSlice) == 0 {
		return "null"
	}

	e := &bytes.Buffer{}
	if useQuotes {
		e.WriteByte('"')
	}
	if len(byteSlice) < 1024 {
		// for small buffers, using Encode directly is much faster.
		dst := make([]byte, base64.StdEncoding.EncodedLen(len(byteSlice)))
		base64.StdEncoding.Encode(dst, byteSlice)
		e.Write(dst)
	} else {
		// for large buffers, avoid unnecessary extra temporary
		// buffer space.
		enc := base64.NewEncoder(base64.StdEncoding, e)
		enc.Write(byteSlice)
		enc.Close()
	}
	if useQuotes {
		e.WriteByte('"')
	}
	return string(e.Bytes())
}

func encodeString(s string) string {
	e := bytes.Buffer{}
	e.WriteByte('"')
	start := 0
	for i := 0; i < len(s); {
		if b := s[i]; b < utf8.RuneSelf {
			if 0x20 <= b && b != '\\' && b != '"' && b != '<' && b != '>' && b != '&' {
				i++
				continue
			}
			if start < i {
				e.WriteString(s[start:i])
			}
			switch b {
			case '\\', '"':
				e.WriteByte('\\')
				e.WriteByte(b)
			case '\n':
				e.WriteByte('\\')
				e.WriteByte('n')
			case '\r':
				e.WriteByte('\\')
				e.WriteByte('r')
			case '\t':
				e.WriteByte('\\')
				e.WriteByte('t')
			default:
				// This encodes bytes < 0x20 except for \n and \r,
				// as well as <, > and &. The latter are escaped because they
				// can lead to security holes when user-controlled strings
				// are rendered into JSON and served to some browsers.
				e.WriteString(`\u00`)
				e.WriteByte(hex[b>>4])
				e.WriteByte(hex[b&0xF])
			}
			i++
			start = i
			continue
		}
		c, size := utf8.DecodeRuneInString(s[i:])
		if c == utf8.RuneError && size == 1 {
			if start < i {
				e.WriteString(s[start:i])
			}
			e.WriteString(`\ufffd`)
			i += size
			start = i
			continue
		}
		// U+2028 is LINE SEPARATOR.
		// U+2029 is PARAGRAPH SEPARATOR.
		// They are both technically valid characters in JSON strings,
		// but don't work in JSONP, which has to be evaluated as JavaScript,
		// and can lead to security holes there. It is valid JSON to
		// escape them, so we do so unconditionally.
		// See http://timelessrepo.com/json-isnt-a-javascript-subset for discussion.
		if c == '\u2028' || c == '\u2029' {
			if start < i {
				e.WriteString(s[start:i])
			}
			e.WriteString(`\u202`)
			e.WriteByte(hex[c&0xF])
			i += size
			start = i
			continue
		}
		i += size
	}
	if start < len(s) {
		e.WriteString(s[start:])
	}
	e.WriteByte('"')
	return string(e.Bytes())
}
