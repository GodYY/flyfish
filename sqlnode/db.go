package node_sql

import (
	"fmt"
	"github.com/jmoiron/sqlx"
)

func pgsqlOpen(host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%d dbname=%s user=%s password=%s sslmode=disable", host, port, dbname, user, password)
	return sqlx.Open("postgres", connStr)
}

func mysqlOpen(host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, password, host, port, dbname)
	return sqlx.Open("mysql", connStr)
}

func dbOpen(sqlType string, host string, port int, dbname string, user string, password string) (*sqlx.DB, error) {
	if sqlType == "mysql" {
		return mysqlOpen(host, port, dbname, user, password)
	} else {
		return pgsqlOpen(host, port, dbname, user, password)
	}
}

func dbOpenByConfig() (*sqlx.DB, error) {
	dbConfig := getNodeConfig().DBConfig
	return dbOpen(dbConfig.SqlType, dbConfig.ConfDbHost, dbConfig.ConfDbPort, dbConfig.ConfDataBase, dbConfig.ConfDbUser, dbConfig.ConfDbPassword)
}

var (
	globalDB *sqlx.DB
)

func initDB() error {
	var err error
	if globalDB, err = dbOpenByConfig(); err != nil {
		return err
	}

	conf := getNodeConfig()
	globalDB.SetMaxOpenConns(conf.DBConnections)

	return nil
}

func getGlobalDB() *sqlx.DB {
	return globalDB
}
