package testableDb

func NewSqllibDbConnection(driverName, connectionString string) (BackendConnecter, error) {
	conn, err := newSqllibBackend(driverName, connectionString)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

func NewPgxDbConnection(server string, port uint16, username string, password string, database string) (BackendConnecter, error) {
	conn, err := newPgxBackend(server, port, username, password, database)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
