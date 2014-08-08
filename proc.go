package sqlpg

import (
	"fmt"
	"log"
	"regexp"
	"strings"
)

func Proc(sql string) ProcStmt {
	return ProcStmt{sql: strings.TrimSpace(sql), params: map[string]interface{}{}}
}

type ProcStmt struct {
	sql    string
	format string
	params map[string]interface{}
}

func (p ProcStmt) Set(name string, value interface{}) ProcStmt {
	params := make(map[string]interface{}, 1)
	params[name] = value
	return p.SetParams(params)
}

func (p ProcStmt) SetParams(params map[string]interface{}, only ...string) ProcStmt {
	dst := make(map[string]interface{}, len(p.params))
	for name, value := range p.params {
		dst[name] = value
	}
	if len(only) > 0 {
		log.Println("ONLY", only, p.params)

		for i := range only {
			name := only[i]
			value, exists := params[name]
			if !exists {
				continue
			}
			dst[name] = value
		}
	} else {
		for name, value := range params {
			dst[name] = value
		}
	}
	p.params = dst
	return p
}

func (p ProcStmt) Len() int {
	return len(p.params)
}

func (p ProcStmt) Format(format string) ProcStmt {
	p.format = format
	return p
}

func (p ProcStmt) String(values ...interface{}) string {
	sql, _ := p.ToSql(values...)
	return sql
}

func (p ProcStmt) ToSql(values ...interface{}) (string, []interface{}) {
	sql, values := ReplaceNameHolders(p.sql, p.params, values...)
	if p.format != "" {
		sql = fmt.Sprintf(p.format, sql)
	}
	return sql, values
}

func (p ProcStmt) Get(db Getter, scan interface{}) error {
	sql, args := p.ToSql()
	return db.Get(scan, sql, args)
}

var (
	reNameHolder      = regexp.MustCompile(":[A-Za-z0-9_-]+")
	reEmptyNameHolder = regexp.MustCompile(`([A-Za-z0-9_-]+(\s+)?:=(\s+)?)?:[A-Za-z0-9_-]+((\s+)?,)?|\$\d[\s,]+\)$`)
	reLastNameHolder  = regexp.MustCompile(`(\$\d)[\s,]+\)$`)
	reNoNameHolders   = regexp.MustCompile(`\([\s]+\)$`)
)

func ReplaceNameHolders(sql string, params map[string]interface{}, values ...interface{}) (string, []interface{}) {
	lenValuesStart := len(values)
	nameHolders := reNameHolder.FindAllString(sql, -1)
	for _, name := range nameHolders {
		value, exists := params[name[1:]]
		if !exists {
			continue
		}
		values = append(values, value)
		dollar := fmt.Sprintf("$%d", len(values))
		sql = strings.Replace(sql, name, dollar, -1)
	}
	// replace any empty holders
	log.Println(sql)
	sql = reEmptyNameHolder.ReplaceAllString(sql, "")
	// clean up sql
	if lenValuesStart == len(values) {
		sql = reNoNameHolders.ReplaceAllString(sql, "()")
	} else {
		sql = reLastNameHolder.ReplaceAllString(sql, "$1)")
	}
	return sql, values
}
