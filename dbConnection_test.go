package testableDb

import (
	"strconv"
	"testing"
)

const connectionString string = "server=ardbdev1;user id=gotest;password=go;database=GoTest;encrypt=disable"

func TestDbConnectionEndToEnd(t *testing.T) {
	db, err := NewSqllibDbConnection("mssql", connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	var id int
	var value string
	row := db.QueryRow("select * from TestTable")
	err = row.Scan(&id, &value)
	if err != nil {
		t.Fatal("expected no error")
	}
	if id != 1 || value != "hello" {
		t.Fatal("expected 1 and value, actual", id, value)
	}

	id = 0
	value = ""
	rows, err := db.Query("select * from TestTable")
	rows.Next()
	err = rows.Scan(&id, &value)
	if err != nil {
		t.Fatal("expected no error")
	}
	rows.Close()
	if id != 1 || value != "hello" {
		t.Fatal("expected 1 and value, actual", id, value)
	}
}

func TestInitDatabaseBogusDriver(t *testing.T) {
	_, err := NewSqllibDbConnection("bogus", connectionString)
	if err == nil {
		t.Fatal("expected to error")
	}
}

func TestInitDatabaseBogusServer(t *testing.T) {
	_, err := NewSqllibDbConnection("mssql", "server=192.168.1.255;Integrated Security=true;Connection Timeout=1;database=GoTest;encrypt=disable")
	if err == nil {
		t.Fatal("expected to error")
	}
}

type Statement struct {
	query string
	rows  RowScanner
	err   error
}

func TestConcurrentQuery(t *testing.T) {
	db, err := NewSqllibDbConnection("mssql", connectionString)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	c := make(chan *Statement)
	for i := 0; i < 50; i++ {
		go func(c chan *Statement, i int) {
			// yes, I know this wouldn't be a real query we'd use since we'd use ?
			query := "select * from MyTable Where CompanyId = 1 and Id = " + strconv.Itoa(i%10)
			rows, err := db.Query(query)
			c <- &Statement{query, rows, err}
		}(c, i)
	}
	results := [50]*Statement{}
	for i := 0; i < 50; i++ {
		results[i] = <-c
		if results[i].err != results[i%10].err || results[i].query[0:51] != results[i%10].query[0:51] || results[i].rows != results[i%10].rows {
			t.Fatal("expected statements to match since we're repeating numbers", results[i], results[i%10])
		}
	}
}
