package onedb

import (
	"testing"
	"time"
)

func TestGetCSVValue(t *testing.T) {
	checkCSVValue(t, 10, nil, "10")
	checkCSVValue(t, "aaaaaa", nil, "aaaaaa")
	checkCSVValue(t, [3]int{2, 1, 9}, nil, "[2 1 9]")
	date := time.Date(2014, 12, 15, 21, 8, 15, 224336449, time.UTC)
	checkCSVValue(t, date, nil, "2014-12-15 21:08:15.224")
	options := map[string]bool{"dateOnly": true}
	checkCSVValue(t, date, options, "2014-12-15")
}

func checkCSVValue(t *testing.T, value interface{}, options map[string]bool, expected string) {
	actual := getCSVValue(&value, options)
	if actual != expected {
		t.Errorf("expected \"%s\", got \"%s\"", expected, actual)
	}
}
