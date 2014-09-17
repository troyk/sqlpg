package sqlpg

import (
	"fmt"
	"reflect"

	"database/sql/driver"
	"strings"
	"sync"

	"github.com/jmoiron/sqlx/reflectx"
)

type columnSchema struct {
	Name    string
	Type    string
	NotNull bool
	Pkey    bool
	Default string
}

type tableSchema struct {
	Name        string
	Columns     []columnSchema
	PrimaryKeys []columnSchema
}

var (
	schemaMutex        sync.RWMutex
	schemaData         = make(map[Driver]map[string]*tableSchema)
	schemaStructMapper = reflectx.NewMapperFunc("json", strings.ToLower)
)

// maps a struct or map[string]interface to map[string]reflect.Value
func fieldMap(value interface{}) map[string]reflect.Value {
	// attempt to map from map[string]
	v := reflect.ValueOf(value)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	if v.Kind() == reflect.Map {
		keys := v.MapKeys()
		fm := make(map[string]reflect.Value, len(keys))
		for _, key := range keys {
			fm[key.Interface().(string)] = v.MapIndex(key)
		}
		return fm
	}
	// default to struct, will panic if not struct
	return schemaStructMapper.FieldMap(reflect.ValueOf(value))

}

func (c columnSchema) ToValueHolder(position int) string {
	var posname, holder, emptyval string
	emptyval = c.EmptyValue()
	if position > 0 {
		posname = fmt.Sprintf("$%d", position)
	} else {
		posname = ":" + c.Name
	}
	if strings.Index(emptyval, "::") > 0 {
		// if emptyval is cast, need to cast positional arg as well
		posname = posname + "::" + c.Type
	}

	if c.NotNull && c.Default != "" {
		holder = fmt.Sprintf("coalesce(nullif(%s,%s)::%s,%s)", posname, emptyval, c.Type, c.Default)
	} else if c.NotNull == false {
		holder = fmt.Sprintf("nullif(%s,%s)::%s", posname, emptyval, c.Type)
	} else {
		holder = posname
	}
	return holder
}

func (c columnSchema) ToValue(value interface{}) interface{} {
	if _, ok := value.(driver.Valuer); ok || value == nil {
		return value
	}
	switch v := value.(type) {
	case []string:
		return StringSlice(v)
	case []int:
		return IntSlice(v)
	}
	return value

}

func (c columnSchema) EmptyValue() string {
	switch c.Type {
	case "timestamptz", "timestamp":
		return "'0001-01-01 00:00:00 zulu'"
	case "integer":
		return "0"
	case "boolean":
		return "false"
	case "text[]", "citext[]", "integer[]":
		return "ARRAY[]::" + c.Type
	}
	return "''"
}

func (t tableSchema) ColumnsByName(names ...string) []columnSchema {
	// zero len names will return all columns by default
	if len(names) == 0 {
		return t.Columns
	}
	namemap := map[string]bool{}
	matched := []columnSchema{}
	for _, name := range strings.Split(strings.Join(names, ","), ",") {
		namemap[strings.TrimSpace(name)] = true
	}
	for _, col := range t.Columns {
		if namemap[col.Name] == true {
			matched = append(matched, col)
		}
	}
	return matched
}

func GetSchema(db Driver) (map[string]*tableSchema, error) {
	schemaMutex.Lock()
	if schemaData[db] != nil {
		schemaMutex.Unlock()
		return schemaData[db], nil
	}
	defer schemaMutex.Unlock()
	schema := map[string]*tableSchema{}
	rows, err := db.Query(`
    SELECT 
       c.relname as table,
       a.attname as col, 
       format_type(a.atttypid, a.atttypmod) as type, 
       --a.atttypid as type_oid, 
       a.attnotnull as notnull,
       coalesce(i.indisprimary,false) as pkey,
       coalesce(d.adsrc,'') as default       
    FROM pg_attribute a 
    LEFT JOIN pg_attrdef d ON a.attrelid = d.adrelid AND a.attnum = d.adnum 
    LEFT JOIN pg_class c ON a.attrelid = c.oid AND c.relkind = 'r' 
    LEFT JOIN pg_namespace ns ON c.relnamespace = ns.oid
    LEFT JOIN pg_index i ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey) 
    WHERE ns.nspname IN ('public') AND a.attnum > 0 AND NOT a.attisdropped ORDER BY c.oid, a.attnum
  `)

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {

		var (
			tableName string
			c         columnSchema
		)
		if err = rows.Scan(&tableName, &c.Name, &c.Type, &c.NotNull, &c.Pkey, &c.Default); err != nil {
			return nil, err
		}
		// normalize type names
		switch c.Type {
		case "timestamp with time zone":
			c.Type = "timestamptz"
		}
		if _, exists := schema[tableName]; !exists {
			schema[tableName] = &tableSchema{Name: tableName, Columns: []columnSchema{}, PrimaryKeys: []columnSchema{}}
		}
		schema[tableName].Columns = append(schema[tableName].Columns, c)
		if c.Pkey {
			schema[tableName].PrimaryKeys = append(schema[tableName].PrimaryKeys, c)
		}

	}
	schemaData[db] = schema
	return schema, nil

}
func GetTableSchema(db Driver, tableName string) (*tableSchema, error) {
	var t *tableSchema
	if schema, err := GetSchema(db); err != nil {
		return nil, err
	} else {
		schemaMutex.Lock()
		t = schema[tableName]
		schemaMutex.Unlock()
	}
	if t == nil {
		return nil, fmt.Errorf("no schema for table %s", tableName)
	}
	return t, nil
}
