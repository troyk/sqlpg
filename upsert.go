package sqlpg

import (
	"bytes"
	"fmt"
	"log"
	"reflect"
	"strings"
)

type upsertBuilder struct {
	schema   *tableSchema
	columns  []string
	holders  []string
	values   []interface{}
	valuemap map[string]reflect.Value
}

func (b *upsertBuilder) Set(data interface{}, only ...string) *upsertBuilder {
	b.valuemap = fieldMap(data)
	b.columns = make([]string, 0)
	b.holders = make([]string, 0)
	b.values = make([]interface{}, 0)
	//log.Println(valuemap)
	position := 0
	for _, col := range b.schema.ColumnsByName(only...) {
		if v, exists := b.valuemap[col.Name]; exists {
			position++
			//log.Println(position, col.Name, col.Type, v, col.ToValueHolder(position))
			b.columns = append(b.columns, col.Name)
			b.values = append(b.values, col.ToValue(v.Interface()))
			b.holders = append(b.holders, col.ToValueHolder(position))
		}
	}
	return b
}

func (b *upsertBuilder) Next(data interface{}) *upsertBuilder {
	if b.columns == nil {
		return b.Set(data)
	}
	b.valuemap = fieldMap(data)
	b.values = make([]interface{}, 0)
	position := 0
	for _, col := range b.schema.Columns {
		if v, exists := b.valuemap[col.Name]; exists {
			position++
			log.Println(col.Name, col.Type, v)
			b.values = append(b.values, col.ToValue(v.Interface()))
		}
	}
	return b
}

func (b *upsertBuilder) InsertSql(data interface{}) (string, []interface{}) {
	b.Set(data)
	sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES(%s)", b.schema.Name, strings.Join(b.columns, ","), strings.Join(b.holders, ","))
	return sql, b.values
}

func (b *upsertBuilder) UpdateSql(data interface{}, pkey interface{}, only ...string) (string, []interface{}) {
	b.Set(data, only...)
	var (
		buffer bytes.Buffer
		last   = len(b.columns) - 1
	)
	buffer.WriteString("UPDATE " + b.schema.Name + " SET ")
	for index, column := range b.columns {
		buffer.WriteString(column + " = " + b.holders[index])
		if index < last {
			buffer.WriteString(",")
		}
	}
	if len(b.schema.PrimaryKeys) > 1 {
		log.Panicf("compound keys not yet implementd (table: %s)", b.schema.Name)
	}
	b.values = append(b.values, pkey)
	buffer.WriteString(fmt.Sprintf(" WHERE %s=$%d", b.schema.PrimaryKeys[0].Name, len(b.values)))
	return buffer.String(), b.values
}

func newUpsertBuilder(db Driver, table string) (*upsertBuilder, error) {
	if schema, err := GetTableSchema(db, table); err != nil {
		return nil, err
	} else {
		return &upsertBuilder{schema: schema}, nil
	}
}
