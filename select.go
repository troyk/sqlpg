// SelectStmts helps you combine arbitrary bits of SQL
// Adapted from github.com/mipearson/sqlc to be pg specific

package sqlpg

import (
	"bytes"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type component struct {
	partial string
	args    []interface{}
}

/*
From the psql documentation:
SELECT [ ALL | DISTINCT [ ON ( expression [, ...] ) ] ]
    * | expression [ [ AS ] output_name ] [, ...]
    [ FROM from_item [, ...] ]
    [ WHERE condition ]
    [ GROUP BY expression [, ...] ]
    [ HAVING condition [, ...] ]
    [ WINDOW window_name AS ( window_definition ) [, ...] ]
    [ { UNION | INTERSECT | EXCEPT } [ ALL | DISTINCT ] select ]
    [ ORDER BY expression [ ASC | DESC | USING operator ] [ NULLS { FIRST | LAST } ] [, ...] ]
    [ LIMIT { count | ALL } ]
    [ OFFSET start [ ROW | ROWS ] ]
    [ FETCH { FIRST | NEXT } [ count ] { ROW | ROWS } ONLY ]
    [ FOR { UPDATE | NO KEY UPDATE | SHARE | KEY SHARE } [ OF table_name [, ...] ] [ NOWAIT ] [...] ]
*/

// Full select API, e.g. s := Select("u.*").From("users u").Where("name = $1", "troy")
func Select(partial string, args ...interface{}) SelectStmt {
	return SelectStmt{selects: []component{component{partial, args}}}
}

// Designed for programatic control over an sql statement from the right side of where
// e.g  s := SelectLiteral("SELECT u.* FROM users u").Where("name = $1", "troy")
func SelectFrom(sqlFrom string) SelectFromStmt {
	return SelectFromStmt{selectSql: sqlFrom}
}

// Begins a SelectFromStmt with the Where clause
func SelectWhere(partial string, args ...interface{}) SelectFromStmt {
	return SelectFromStmt{}.Where(partial, args...)
}

// SelectStmt is a SQL string being built
type SelectStmt struct {
	selects []component
	froms   []component
	joins   []component
	wheres  []component
	groups  []component
	havings []component
	orders  []component
	limit   int
}

// Select adds a SELECT stanza, joined by commas
func (s SelectStmt) Select(partial string, args ...interface{}) SelectStmt {
	s.selects = append(s.selects, component{partial, args})
	return s
}

// From adds a FROM stanza, joined by commas
func (s SelectStmt) From(partial string, args ...interface{}) SelectStmt {
	s.froms = append(s.froms, component{partial, args})
	return s
}

// Join adds a JOIN stanza, joined by spaces
// Unlike other stanzas, you need to specify the JOIN/INNER JOIN/LEFT JOIN bit yourself.
func (s SelectStmt) Join(partial string, args ...interface{}) SelectStmt {
	s.joins = append(s.joins, component{partial, args})
	return s
}

// Where adds a WHERE stanza, wrapped in brackets and joined by AND
func (s SelectStmt) Where(partial string, args ...interface{}) SelectStmt {
	s.wheres = append(s.wheres, component{"(" + partial + ")", args})
	return s
}

// Having adds a HAVING stanza, wrapped in brackets and joined by AND
func (s SelectStmt) Having(partial string, args ...interface{}) SelectStmt {
	s.havings = append(s.havings, component{"(" + partial + ")", args})
	return s
}

// Group adds a GROUP BY stanza, joined by commas
func (s SelectStmt) Group(partial string, args ...interface{}) SelectStmt {
	s.groups = append(s.groups, component{partial, args})
	return s
}

// Order adds an ORDER BY stanza, joined by commas
func (s SelectStmt) Order(partial string, args ...interface{}) SelectStmt {
	s.orders = append(s.orders, component{partial, args})
	return s
}

// Limit sets or overwrites the LIMIT stanza
func (s SelectStmt) Limit(limit int) SelectStmt {
	s.limit = limit
	return s
}

func (s SelectStmt) String(previousArgs ...interface{}) string {
	sql, _ := s.ToSql(previousArgs...)
	return sql
}

// Args returns positional arguments in the order they will appear in the SQL.
func (s SelectStmt) Args(previousArgs ...interface{}) []interface{} {
	_, args := s.ToSql(previousArgs...)
	return args
}

// SQL joins your stanzas, returning the composed SQL.
func (s SelectStmt) ToSql(args ...interface{}) (string, []interface{}) {
	sql := &bytes.Buffer{}

	if len(s.selects) > 0 {
		sql.WriteString("SELECT ")
		writeComponents(sql, s.selects, ", ", &args)
	}
	if len(s.froms) > 0 {
		sql.WriteString("\nFROM ")
		writeComponents(sql, s.froms, ", ", &args)
	}
	if len(s.joins) > 0 {
		// joins may or may not include the JOIN prefix, so we need to add it only if missing
		for _, join := range s.joins {
			words := strings.Fields(strings.ToUpper(join.partial))
			if len(words) > 2 && (words[0] == "JOIN" || words[1] == "JOIN") {
				sql.WriteString("\n")
			} else {
				sql.WriteString("\nJOIN ")
			}
			writeComponents(sql, []component{join}, "", &args)
		}
	}
	if len(s.wheres) > 0 {
		sql.WriteString("\nWHERE ")
		writeComponents(sql, s.wheres, " AND ", &args)
	}
	if len(s.groups) > 0 {
		sql.WriteString("\nGROUP BY ")
		writeComponents(sql, s.groups, ", ", &args)
	}
	if len(s.havings) > 0 {
		sql.WriteString("\nHAVING ")
		writeComponents(sql, s.havings, " AND ", &args)
	}
	if len(s.orders) > 0 {
		sql.WriteString("\nORDER BY ")
		writeComponents(sql, s.orders, ", ", &args)
	}
	if s.limit > 0 {
		sql.WriteString(fmt.Sprintf("\nLIMIT %d", s.limit))
	}

	return sql.String(), args
}

// SelectStmt is like SelectStmt only select, from and join are static strings
type SelectFromStmt struct {
	selectSql string
	format    string
	stmt      SelectStmt
}

// Sets the selectSql
func (s SelectFromStmt) Select(selectSql string) SelectFromStmt {
	s.selectSql = selectSql
	return s
}

// Where adds a WHERE stanza, wrapped in brackets and joined by AND
func (s SelectFromStmt) Where(partial string, args ...interface{}) SelectFromStmt {
	s.stmt = s.stmt.Where(partial, args...)
	return s
}

// Having adds a HAVING stanza, wrapped in brackets and joined by AND
func (s SelectFromStmt) Having(partial string, args ...interface{}) SelectFromStmt {
	s.stmt = s.stmt.Having(partial, args...)
	return s
}

// Group adds a GROUP BY stanza, joined by commas
func (s SelectFromStmt) Group(partial string, args ...interface{}) SelectFromStmt {
	s.stmt = s.stmt.Group(partial, args...)
	return s
}

// Order adds an ORDER BY stanza, joined by commas
func (s SelectFromStmt) Order(partial string, args ...interface{}) SelectFromStmt {
	s.stmt = s.stmt.Order(partial, args...)
	return s
}

// Limit sets or overwrites the LIMIT stanza
func (s SelectFromStmt) Limit(limit int) SelectFromStmt {
	s.stmt = s.stmt.Limit(limit)
	return s
}

// Only sets the limit if limit is not already set
func (s SelectFromStmt) LimitOr(limit int) SelectFromStmt {
	if s.stmt.limit == 0 {
		return s.Limit(limit)
	}
	return s
}

// Sprintf format string used when producing sql string output
func (s SelectFromStmt) Format(format string) SelectFromStmt {
	s.format = format
	return s
}

func (s SelectFromStmt) String(args ...interface{}) string {
	sql, _ := s.ToSql(args...)
	return sql
}

// Args returns positional arguments in the order they will appear in the SQL.
func (s SelectFromStmt) Args(args ...interface{}) []interface{} {
	return s.stmt.Args(args...)
}

// SQL joins your stanzas, returning the composed SQL.
func (s SelectFromStmt) ToSql(args ...interface{}) (string, []interface{}) {
	sql, allArgs := s.stmt.ToSql(args...)
	sql = s.selectSql + sql
	if s.format != "" {
		sql = fmt.Sprintf(s.format, sql)
	}
	return sql, allArgs
}

var reDollarPosition = regexp.MustCompile(`\$\d{1,4}\b`)
var reQuoted = regexp.MustCompile(`\'[^\']*\'`)

func writeComponents(w *bytes.Buffer, components []component, joiner string, args *[]interface{}) {
	for componentIndex, component := range components {
		if componentIndex > 0 {
			w.WriteString(joiner)
		}
		argOffset := len(*args)
		// rip out all the quoted literals so they do not interefere with dollar
		// placeholder substitution
		quotes := make([]string, 0)
		sql := reQuoted.ReplaceAllStringFunc(component.partial, func(s string) string {
			quotes = append(quotes, s)
			return fmt.Sprintf(`/*%d*/`, len(quotes)-1)
		})
		// replace dollar signs (which are scoped to their compenonent and then
		// adjusted to match the positional index of all args
		sql = reDollarPosition.ReplaceAllStringFunc(sql, func(s string) string {
			pos, _ := strconv.Atoi(s[1:])
			if pos > 0 {
				return fmt.Sprintf("$%d", pos+argOffset)
			}
			return s
		})
		// put the quotes back, mama loves cleanup time...
		for index, quote := range quotes {
			sql = strings.Replace(sql, fmt.Sprintf(`/*%d*/`, index), quote, 1)
		}

		w.WriteString(sql)
		*args = append(*args, component.args...)

	}

	return
}
