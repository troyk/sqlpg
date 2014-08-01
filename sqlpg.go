package sqlpg

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	_ "github.com/lib/pq"
)

// Public driver with enhanched methods
type Driver interface {
	// enhanced
	Get(scan interface{}, query string, args ...interface{}) error
	GetInt(query string, args ...interface{}) (result int64, err error)
	GetString(query string, args ...interface{}) (result string, err error)
	Rollback() error

	// Go
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
	Begin() (Driver, error)
}

// Private driver, clones go's (missing) driver interface
type goDriver interface {
	Rollback() error
	QueryRow(query string, args ...interface{}) *sql.Row
	Exec(query string, args ...interface{}) (sql.Result, error)
}

type Queryer interface {
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

// Wrap sql.DB
type DB struct {
	*sql.DB
}

// Go currently has no notion of nested transcations so we
// implement our own savepoints, this may be addressed in a
// later version of go, or never as support differs between
// db systems.  Since we are focused on postgres, we don't
// care
type Tx struct {
	*sql.Tx
	savepoint int
}

// If begin is called from a tx, we set a save point
func (tx *Tx) Begin() (Driver, error) {
	rand.Seed(time.Now().UTC().UnixNano())
	savepoint := rand.Int()
	_, err := tx.Exec(fmt.Sprintf("SAVEPOINT savepoint%d", savepoint))
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx.Tx, savepoint: savepoint}, nil
}

func (tx *Tx) Commit() error {
	if tx.savepoint != 0 {
		if _, err := tx.Exec(fmt.Sprintf("RELEASE SAVEPOINT savepoint%d", tx.savepoint)); err != nil {
			return err
		}
		tx.savepoint = 0
		return nil
	}
	return tx.Tx.Commit()
}

func (tx *Tx) Rollback() error {
	if tx.savepoint != 0 {
		if _, err := tx.Exec(fmt.Sprintf("ROLLBACK TO SAVEPOINT savepoint%d", tx.savepoint)); err != nil {
			return err
		}
		tx.savepoint = 0
		return nil
	}
	return tx.Tx.Rollback()
}

// Wrap sql.DB with sqlpg funcs
func (db *DB) Begin() (Driver, error) {
	tx, err := db.DB.Begin()
	if err != nil {
		return nil, err
	}
	return &Tx{Tx: tx}, nil
}

// Go sql.DB has no rollback method, but we create a no-op to make the
// db interchangable with a sql.Tx
func (db *DB) Rollback() error {
	// thie really should not happen so going to see how panic works here
	panic(fmt.Errorf("db.Rollback() called, should only happen on sql.Tx so transaction state more than likely corrupted"))
}

// db
func (db *DB) Get(scan interface{}, query string, args ...interface{}) error {
	return Get(db, scan, query, args...)
}

func (db *DB) GetInt(query string, args ...interface{}) (result int64, err error) {
	return GetInt(db, query, args...)
}

func (db *DB) GetString(query string, args ...interface{}) (result string, err error) {
	return GetString(db, query, args...)
}

// tx
func (db *Tx) Exec(query string, args ...interface{}) (sql.Result, error) {
	return Exec(db.Tx, query, args...)
}

func (db *Tx) Get(scan interface{}, query string, args ...interface{}) error {
	return Get(db, scan, query, args...)
}

func (db *Tx) GetInt(query string, args ...interface{}) (result int64, err error) {
	return GetInt(db, query, args...)
}

func (db *Tx) GetString(query string, args ...interface{}) (result string, err error) {
	return GetString(db, query, args...)
}

func Exec(db goDriver, query string, args ...interface{}) (sql.Result, error) {
	log.Println("EXEC: ", query, args)
	return db.Exec(query, args...)
}

func Get(db Queryer, scan interface{}, query string, args ...interface{}) error {
	var data []byte
	err := db.QueryRow(query, args...).Scan(&data)
	switch {
	case err == sql.ErrNoRows:
		return nil
	case err != nil:
		err = errors.New(fmt.Sprintf("%s:\n\n%s", err, string(data)))
		return err
	default:
		log.Println(string(data))
		err = json.Unmarshal(data, scan)
		if err != nil {
			str := string(data)
			if str == "" {
				return nil
			}
		}
		return err
	}
}

func GetInt(db Queryer, query string, args ...interface{}) (result int64, err error) {
	err = db.QueryRow(query, args...).Scan(&result)
	if err == sql.ErrNoRows {
		return result, nil
	}
	return result, err
}

func GetString(db Queryer, query string, args ...interface{}) (result string, err error) {
	err = db.QueryRow(query, args...).Scan(&result)
	//log.Println("GetString", query, args, result, err)
	if err == sql.ErrNoRows {
		return result, nil
	}
	return result, err
}

func Open(dataSourceName string) (*DB, error) {
	pg, err := sql.Open("postgres", dataSourceName)
	if err != nil {
		return nil, err
	}
	//templates := &SqlTemplate{Template: template.New("schema")}
	return &DB{DB: pg}, nil
}

//with ad as (
//  select ads.* from ads where ads.id = '2ec72d3f-4665-461a-841d-faeee4e199c6'
//),
//business as (
//  select b.*,
//  (select json_agg(c) from categories c where c.id IN (select bc.category_id from businesses_categories bc where bc.business_id = b.id)) as categories
//  from businesses b join ad on b.id=ad.business_id
//) select to_json(row) FROM (select ad.*, business from ad left join business on ad.business_id = business.id) row;

//with ad as (
//  select ads.* from ads where ads.id = '2ec72d3f-4665-461a-841d-faeee4e199c6'
//),
//business as (
//  select b.*,
//  (select json_agg(c) from categories c where c.id IN (select bc.category_id from businesses_categories bc where bc.business_id = b.id)) as categories
//  from businesses b join ad on b.id=ad.business_id
//) select to_json(row) FROM (select ad.* FROM ad) row;

//select to_json(row) from (
//  select ads.*,
//  (select business from (
//    select b.*,
//    (select json_agg(c) from categories c where c.id IN (select bc.category_id FROM businesses_categories bc where bc.business_id = b.id )) as categories
//    from businesses b where b.id=ads.business_id
//  ) business)
//  from ads
//  where ads.id = '2ec72d3f-4665-461a-841d-faeee4e199c6'
//) row;

//select to_json(row) from (
//  select ads.*,
//  (select ROW(json_agg(b), json_agg(c)) from businesses b join businesses_categories bc on b.id = bc.business_id join categories c on c.id = bc.category_id where b.id=ads.business_id)
//  --,(select json_agg(categories) from categories where false AND categories.id IN (select bc.category_id FROM businesses_categories bc where bc.business_id = businesses.id )) as categories
//  from ads
//  where ads.id = '865d35a2-d038-4558-94dc-adc73ffb3210')
// row;
