// Package sqllsh provides an on-disk alternative to in-memory LSH index,
// using relational databases.
// This package does not implement any specific locality-sensitive
// hash function family
// (e.g. MinHash, Random Hyperplane Projection, p-Stable Distribution Projection, etc.).
// It lets you store the hash values with the k and l parameters.
// where k is the number of hash values that form one hash key,
// and l is the number of hash tables that uses the hash keys.
// For detail of the algorithm one can read this book chapter:
// http://infolab.stanford.edu/~ullman/mmds/ch3.pdf
//
// Inside a relational database, an LSH index is a table,
// each Signature is a row, and each locality-sensitive hash function's hash values
// form a column.
// During query, collisons of hash keys are checked using AND and OR.
// A B-Tree multi-column index can be built for each hash key
// to improve query performance.
package sqllsh

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
)

// Signature is a list of integer hash values from
// their corresponding locality-sensitive hash functions.
// Since this library does not include the hash functions,
// the hash values are used directly as input.
type Signature []uint

// SqlLsh is the entry point to the on-disk LSH index.
type SqlLsh struct {
	k              int              // Hash key size
	l              int              // Number of hash tables, or number of hash keys
	tableName      string           // Name of the database table used
	db             *sql.DB          // Database connection
	varFmt         func(int) string // Database specific formatter for placehoder
	insertStmt     *sql.Stmt
	queryStmt      *sql.Stmt
	scanStmt       *sql.Stmt
	indexStmts     []*sql.Stmt
	createIndexFmt string
}

func newSqlLsh(k, l int, tableName string, db *sql.DB,
	varFmt func(int) string,
	createIndexFmt string) (*SqlLsh, error) {
	lsh := &SqlLsh{
		k:              k,
		l:              l,
		tableName:      tableName,
		db:             db,
		varFmt:         varFmt,
		createIndexFmt: createIndexFmt,
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
	lsh.scanStmt, err = lsh.createScanStmt()
	if err != nil {
		return nil, err
	}
	lsh.indexStmts, err = lsh.createIndexStmts()
	if err != nil {
		return nil, err
	}
	return lsh, nil
}

// Index builds l B-Tree multi-column indexes, each covers a
// concatenated hash key.
// This can improve the query performance of the LSH index.
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

// Insert appends a new Signature with id to the table.
// The size of the new Signature must equal to k*l.
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

// BatchInsert appends a list of Signatures to the table.
// Each id in the list ids corresponds to the ID of the Signature at the
// same position.
// BatchInsert is more efficient than Insert for inserting multiple
// Signatures at the same time.
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

// Query finds the IDs of the Signatures that have at least one
// hash key collison with the query Signature, then writes the
// IDs to a given output channel.
// The caller is responsible for closing the channel.
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
	defer rows.Close()
	for rows.Next() {
		var id int
		err = rows.Scan(&id)
		if err != nil {
			return err
		}
		out <- id
	}
	err = rows.Err()
	return err
}

type Entry struct {
	Id        int
	Signature Signature
}

func (lsh *SqlLsh) Scan(out chan Entry) error {
	row := make([]interface{}, lsh.k*lsh.l+1)
	rowPtr := make([]interface{}, lsh.k*lsh.l+1)
	for i := range row {
		rowPtr[i] = &row[i]
	}
	rows, err := lsh.queryStmt.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		if err := rows.Scan(rowPtr...); err != nil {
			return err
		}
		id := row[0].(int)
		sig := make(Signature, len(row)-1)
		for i := range sig {
			sig[i] = row[i+1].(uint)
		}
		out <- Entry{
			Id:        id,
			Signature: sig,
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
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
		stmt, err := lsh.db.Prepare(fmt.Sprintf(lsh.createIndexFmt, i, lsh.tableName) +
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
	stmt, err := lsh.db.Prepare(fmt.Sprintf("SELECT DISTINCT id FROM %s WHERE", lsh.tableName) +
		strings.Join(querySeg, " OR ") + ";")
	return stmt, err
}

func (lsh *SqlLsh) createScanStmt() (*sql.Stmt, error) {
	return lsh.db.Prepare(fmt.Sprintf("SELECT * FROM %s", lsh.tableName))
}
