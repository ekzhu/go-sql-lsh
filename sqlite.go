package sqllsh

import "database/sql"

func NewSqliteLsh(k, l int, tableName string, db *sql.DB) (*SqlLsh, error) {
	varFmt := func(i int) string {
		return "?"
	}
	lsh, err := NewSqlLsh(k, l, tableName, db, varFmt)
	return lsh, err
}
