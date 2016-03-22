package sqllsh

import (
	"database/sql"
	"io/ioutil"
	"log"
	"math"
	"os"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

func creatTempFileBench(t *testing.B) *os.File {
	tmpfile, err := ioutil.TempFile("", "_test")
	if err != nil {
		t.Fatal(err)
	}
	return tmpfile
}

func removeTempFileBench(t *testing.B, tempfile *os.File) {
	if err := tempfile.Close(); err != nil {
		t.Fatal(err)
	}
}

func runInsert(k, l int, b *testing.B) {
	sigs := randomSigs(b.N, k*l, math.MaxFloat64)
	f := creatTempFileBench(b)
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		b.Fatal(err)
	}
	lsh, err := NewSqliteLsh(k, l, "lshtable", db)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]int, len(sigs))
	for i := range sigs {
		ids[i] = i
	}
	b.ResetTimer()

	start := time.Now()
	err = lsh.BatchInsert(ids, sigs)
	if err != nil {
		b.Fatal(err)
	}
	dur := float64(time.Now().Sub(start)) / float64(time.Second)
	log.Printf("Batch inserting %d signatures takes %.4f seconds", len(sigs), dur)
	start = time.Now()
	lsh.Index()
	dur = float64(time.Now().Sub(start)) / float64(time.Second)
	log.Printf("Building index takes %.4f seconds", dur)
	if err != nil {
		b.Fatal(err)
	}

	removeTempFileBench(b, f)
}

func BenchmarkSqliteLsh128(b *testing.B) {
	runInsert(2, 64, b)
}

func BenchmarkSqliteLsh256(b *testing.B) {
	runInsert(4, 64, b)
}

func BenchmarkSqliteLsh512(b *testing.B) {
	runInsert(8, 64, b)
}
