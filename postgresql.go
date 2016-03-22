package sqllsh

import (
	"database/sql"
	"fmt"
)

func NewPostgresLsh(k, l int, tableName string, db *sql.DB) (*SqlLsh, error) {
	varFmt := func(i int) string {
		return fmt.Sprintf("$%d", i+1)
	}
	lsh, err := NewSqlLsh(k, l, tableName, db, varFmt)
	return lsh, err
}
