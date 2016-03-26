package sqllsh

import (
	"database/sql"
	"fmt"
)

// NewPostgresLsh creates a new PostgreSQL-backed LSH index.
// The caller is responsible for closing the database connection
// object.
func NewPostgresLsh(k, l int, tableName string, db *sql.DB) (*SqlLsh, error) {
	varFmt := func(i int) string {
		return fmt.Sprintf("$%d", i+1)
	}
	createIndexFmt := "CREATE INDEX ht_%d ON %s USING BTREE ("
	lsh, err := newSqlLsh(k, l, tableName, db, varFmt, createIndexFmt)
	return lsh, err
}
