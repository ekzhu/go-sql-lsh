package sqllsh

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

type Signature []uint

type SqlLsh struct {
	k          int     // Hash key size, or size of a multi-column index
	l          int     // Number of hash tables, or number of multi-column indexes
	tableName  string  // Name of the database table used
	db         *sql.DB // Database connection
	varFmt     func(int) string
	insertStmt *sql.Stmt
	queryStmt  *sql.Stmt
	indexStmts []*sql.Stmt
}

// NewSqlLsh creates a new LSH index using SQL and multi-column indexes
func NewSqlLsh(k, l int, tableName string, db *sql.DB, varFmt func(int) string) (*SqlLsh, error) {
	lsh := &SqlLsh{
		k:         k,
		l:         l,
		tableName: tableName,
		db:        db,
		varFmt:    varFmt,
	}
	tx, err := db.Begin()
	if err != nil {
		return nil, err
	}
	_, err = tx.Exec(lsh.createTableStr())
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return nil, err
	}
	// Prepare statments for later use
	lsh.insertStmt, err = lsh.createInsertStmt()
	if err != nil {
		return nil, err
	}
	lsh.queryStmt, err = lsh.createQueryStmt()
	if err != nil {
		return nil, err
	}
	lsh.indexStmts, err = lsh.createIndexStmts()
	if err != nil {
		return nil, err
	}
	return lsh, nil
}

func (lsh *SqlLsh) Index() error {
	tx, err := lsh.db.Begin()
	if err != nil {
		return err
	}
	for i := range lsh.indexStmts {
		_, err = tx.Stmt(lsh.indexStmts[i]).Exec()
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func (lsh *SqlLsh) Insert(id int, sig Signature) error {
	if len(sig) != lsh.k*lsh.l {
		return errors.New("Signature size mismatch")
	}
	row := make([]interface{}, len(sig)+1)
	row[0] = interface{}(id)
	for i := 0; i < len(sig); i++ {
		row[i+1] = interface{}(sig[i])
	}
	// Begin transcation for insert
	tx, err := lsh.db.Begin()
	if err != nil {
		return err
	}
	_, err = tx.Stmt(lsh.insertStmt).Exec(row...)
	if err != nil {
		tx.Rollback()
		return err
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func (lsh *SqlLsh) BatchInsert(ids []int, sigs []Signature) error {
	if len(sigs) != len(ids) {
		return errors.New("Number of signatures and ids mismatch")
	}
	if len(sigs[0]) != lsh.k*lsh.l {
		return errors.New("Signature size mismatch")
	}
	// Begin transcation for insert
	tx, err := lsh.db.Begin()
	if err != nil {
		return err
	}
	for i := range sigs {
		row := make([]interface{}, lsh.l*lsh.k+1)
		row[0] = interface{}(ids[i])
		for j := 0; j < len(sigs[i]); j++ {
			row[j+1] = interface{}(sigs[i][j])
		}
		_, err = tx.Stmt(lsh.insertStmt).Exec(row...)
		if err != nil {
			tx.Rollback()
			return err
		}
	}
	err = tx.Commit()
	if err != nil {
		tx.Rollback()
		return err
	}
	return nil
}

func (lsh *SqlLsh) Query(sig Signature, out chan int) error {
	if len(sig) != lsh.k*lsh.l {
		return errors.New("Signature size mismatch")
	}
	row := make([]interface{}, len(sig))
	for i := 0; i < len(sig); i++ {
		row[i] = interface{}(sig[i])
	}
	rows, err := lsh.queryStmt.Query(row...)
	if err != nil {
		return err
	}
	set := make(map[int]bool)
	defer rows.Close()
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			return err
		}
		if _, seen := set[id]; seen {
			continue
		}
		out <- id
		set[id] = true
	}
	err = rows.Err()
	return err
}

func (lsh *SqlLsh) createTableStr() string {
	createSeg := make([]string, lsh.k*lsh.l+1)
	createSeg[0] = "id INTEGER PRIMARY KEY"
	for i := 0; i < lsh.k*lsh.l; i++ {
		createSeg[i+1] = fmt.Sprintf("hv_%d BIGINT", i)
	}
	return fmt.Sprintf("CREATE TABLE %s (\n", lsh.tableName) +
		strings.Join(createSeg, ",\n") + "\n);\n"
}

func (lsh *SqlLsh) createIndexStmts() ([]*sql.Stmt, error) {
	indexStmts := make([]*sql.Stmt, lsh.l)
	seg := make([]string, lsh.k)
	for i := 0; i < lsh.l; i++ {
		for j := 0; j < lsh.k; j++ {
			seg[j] = fmt.Sprintf("hv_%d", lsh.k*i+j)
		}
		stmt, err := lsh.db.Prepare(fmt.Sprintf("CREATE INDEX ht_%d ON %s (", i, lsh.tableName) +
			strings.Join(seg, ",") + ");")
		if err != nil {
			return nil, err
		}
		indexStmts[i] = stmt
	}
	return indexStmts, nil
}

func (lsh *SqlLsh) createInsertStmt() (*sql.Stmt, error) {
	insertSeg := make([]string, lsh.k*lsh.l+1)
	for i := range insertSeg {
		insertSeg[i] = lsh.varFmt(i)
	}
	stmt, err := lsh.db.Prepare(fmt.Sprintf("INSERT INTO %s VALUES(", lsh.tableName) +
		strings.Join(insertSeg, ",") + ");")
	return stmt, err
}

func (lsh *SqlLsh) createQueryStmt() (*sql.Stmt, error) {
	querySeg := make([]string, lsh.l)
	seg := make([]string, lsh.k)
	for i := 0; i < lsh.l; i++ {
		for j := 0; j < lsh.k; j++ {
			k := lsh.k*i + j
			seg[j] = fmt.Sprintf("hv_%d = %s", k, lsh.varFmt(k))
		}
		querySeg[i] = "(" + strings.Join(seg, " AND ") + ")"
	}
	stmt, err := lsh.db.Prepare(fmt.Sprintf("SELECT id FROM %s WHERE", lsh.tableName) +
		strings.Join(querySeg, " OR ") + ";")
	return stmt, err
}
