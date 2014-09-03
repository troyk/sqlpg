package sqlpg

import (
	"strings"

	"encoding/json"

	"github.com/jmoiron/sqlx"
	"github.com/jmoiron/sqlx/reflectx"
	"github.com/lib/pq"
)

type DB struct {
	*sqlx.DB
}

type Driver interface {
	GetJSON(dest interface{}, query string, args ...interface{}) error
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

func GetJSON(q sqlx.Queryer, dest interface{}, query string, args ...interface{}) error {
	var data string
	if err := q.QueryRowx(query, args...).Scan(&data); err != nil {
		return err
	}
	return json.Unmarshal([]byte(data), dest)
}
