package pgsql

import (
	"database/sql"
	"fmt"
	_ "github.com/lib/pq"
	"strings"
	"sync"
	"time"
)

type Client struct {
	tt     string
	dbConn *sql.DB
}

var (
	clients map[string]*Client
	once    = sync.Once{}
)

// postgres/mysql@127.0.0.1@5432@deng@dbuser@123456
func GetClient(dbConfig string) (*Client, error) {
	c, ok := clients[dbConfig]
	if ok {
		return c, nil
	}

	s := strings.Split(dbConfig, "@")
	if len(s) != 6 {
		return nil, fmt.Errorf("%s is failed", dbConfig)
	}

	var db *sql.DB
	var err error
	tt := s[0]
	if tt == "mysql" {
		db, err = mysqlOpen(s[1], s[2], s[3], s[4], s[5])
	} else if tt == "postgres" {
		db, err = pgsqlOpen(s[1], s[2], s[3], s[4], s[5])
	} else {
		return nil, fmt.Errorf("type(%s) is failed", tt)
	}

	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	c = &Client{
		tt:     tt,
		dbConn: db,
	}
	clients[dbConfig] = c

	return c, nil
}

func pgsqlOpen(host string, port string, dbname string, user string, password string) (*sql.DB, error) {
	connStr := fmt.Sprintf("host=%s port=%s dbname=%s user=%s password=%s sslmode=disable", host, port, dbname, user, password)
	return sql.Open("postgres", connStr)
}

func mysqlOpen(host string, port string, dbname string, user string, password string) (*sql.DB, error) {
	connStr := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", user, password, host, port, dbname)
	return sql.Open("mysql", connStr)
}

func init() {
	once.Do(func() {
		clients = map[string]*Client{}
		go func() {
			for {
				for k, c := range clients {
					fmt.Println(k, c)
				}
				time.Sleep(time.Hour)
			}
		}()
	})
}
