package sqlpg

import (
	"database/sql"
	"fmt"
	"log"

	"strings"

	"encoding/json"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/lib/pq"
)

type DB struct {
	*sqlx.DB
}

type Tx struct {
	*sqlx.Tx
}

type Driver interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	GetJSON(dest interface{}, query string, args ...interface{}) error
	Insert(table string, data interface{}) error
}

func (db *DB) Begin() (*Tx, error) {
	tx, err := db.DB.Beginx()
	if err != nil {
		return nil, err
	}
	return &Tx{tx}, nil
}

func (db *DB) Insert(table string, row interface{}) error {
	return Insert(db, table, row)
}
func (tx *Tx) Insert(table string, row interface{}) error {
	return Insert(tx, table, row)
}

func (db *DB) Update(table string, row interface{}, pkey interface{}, only ...string) error {
	return Update(db, table, row, pkey, only...)
}
func (tx *Tx) Update(table string, row interface{}, pkey interface{}, only ...string) error {
	return Update(tx, table, row, pkey, only...)
}

func (db *DB) GetJSON(dest interface{}, query string, args ...interface{}) error {
	return GetJSON(db, dest, query, args...)
}
func (tx *Tx) GetJSON(dest interface{}, query string, args ...interface{}) error {
	return GetJSON(tx, dest, query, args...)
}

func Open(url string) (*DB, error) {
	var err error
	var parsedUrl string
	var db *sqlx.DB

	parsedUrl, err = pq.ParseURL(url)
	if err != nil {
		parsedUrl = url
	}

	db, err = sqlx.Open("postgres", parsedUrl)
	if err != nil {
		return nil, err
	}

	// set sane defaults for postgresql love
	db.Mapper = reflectx.NewMapperFunc("json", strings.ToLower)
	db.SetMaxIdleConns(10)

	return &DB{db}, err
}

func MustOpen(url string) *DB {
	db, err := Open(url)
	if err != nil {
		panic(err)
	}
	return db
}

func GetJSON(q sqlx.Queryer, dest interface{}, query string, args ...interface{}) error {
	var data string
	if err := q.QueryRowx(query, args...).Scan(&data); err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), dest)
}

func Insert(db Driver, table string, data interface{}) error {
	builder, err := newUpsertBuilder(db, table)
	if err != nil {
		return err
	}
	sql, values := builder.InsertSql(data)
	sql = fmt.Sprintf("%s RETURNING to_json(%s.*)", sql, table)
	log.Println("INSERT:", sql, values)
	return db.GetJSON(data, sql, values...)
}

func Update(db Driver, table string, data interface{}, pkey interface{}, only ...string) error {
	builder, err := newUpsertBuilder(db, table)
	if err != nil {
		return err
	}
	sql, values := builder.UpdateSql(data, pkey, only...)
	sql = fmt.Sprintf("%s RETURNING to_json(%s.*)", sql, table)
	return db.GetJSON(data, sql, values...)
}
