package sqlpg

import (
	"database/sql"
	"github.com/jmoiron/sqlx"
	"sort"
	"strconv"
	"strings"
)

func ProcNamedJSON(q sqlx.Queryer, dest interface{}, proc string, params map[string]interface{}) error {
	sqlstr, values := ProcNamedSql(proc, params)
	return GetJSON(q, dest, "SELECT "+sqlstr, values...)
}

func ProcNamedString(q sqlx.Queryer, proc string, params map[string]interface{}) (string, error) {
	sqlstr, values := ProcNamedSql(proc, params)
	resultStr := sql.NullString{}
	err := q.QueryRowx("SELECT "+sqlstr, values...).Scan(&resultStr)
	return resultStr.String, err
}

func ProcNamedSql(proc string, params map[string]interface{}) (string, []interface{}) {
	names := make([]string, 0)
	for name, value := range params {
		if value == nil || IsEmpty(value) == false {
			names = append(names, name)
		}

	}
	// sort by name so query can be checksumed and query cached
	sort.Strings(names)
	values := make([]interface{}, len(names))
	for i, name := range names {
		values[i] = params[name]
		names[i] = name + ":=$" + strconv.Itoa(i+1)
	}
	sqlstr := proc + "(" + strings.Join(names, ",") + ")"
	return sqlstr, values
}
