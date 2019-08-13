package onedb

import (
	"testing"
	"time"
)

func TestGetCSVValue(t *testing.T) {
	var options CSVOptions
	checkCSVValue(t, 10, options, "10")
	checkCSVValue(t, "aaaaaa", options, "aaaaaa")
	checkCSVValue(t, [3]int{2, 1, 9}, options, "[2 1 9]")
	date := time.Date(2014, 12, 15, 21, 8, 15, 224336449, time.UTC)
	checkCSVValue(t, date, options, "2014-12-15 21:08:15.224")
	options.DateOnly = true
	checkCSVValue(t, date, options, "2014-12-15")
}

func checkCSVValue(t *testing.T, value interface{}, options CSVOptions, expected string) {
	actual := getCSVValue(&value, options)
	if actual != expected {
		t.Errorf("expected \"%s\", got \"%s\"", expected, actual)
	}
}
