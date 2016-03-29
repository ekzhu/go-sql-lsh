package sqllsh

import (
	"database/sql"
	"io/ioutil"
	"log"
	"math/rand"
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

func runSqlite(k, l, n, nq int, b *testing.B) {
	// Inialize database
	f := creatTempFileBench(b)
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		b.Fatal(err)
	}

	// Initalize data
	lsh, err := NewSqliteLsh(k, l, "lshtable", db)
	if err != nil {
		b.Fatal(err)
	}
	sigs := randomSigs(n, k*l)
	ids := make([]int, len(sigs))
	for i := range sigs {
		ids[i] = i
	}
	qids := rand.Perm(len(ids))[:nq]

	// Inserting
	start := time.Now()
	err = lsh.BatchInsert(ids, sigs)
	if err != nil {
		b.Fatal(err)
	}
	dur := float64(time.Now().Sub(start)) / float64(time.Second)
	log.Printf("Batch inserting %d signatures takes %.4f seconds", len(sigs), dur)

	// Indexing
	start = time.Now()
	lsh.Index()
	if err != nil {
		b.Fatal(err)
	}
	dur = float64(time.Now().Sub(start)) / float64(time.Second)
	log.Printf("Building index takes %.4f seconds", dur)

	// Query
	start = time.Now()
	for _, i := range qids {
		out := make(chan int)
		go func() {
			err := lsh.Query(sigs[i], out)
			if err != nil {
				b.Error(err)
			}
			close(out)
		}()
		for _ = range out {
		}
	}
	dur = float64(time.Now().Sub(start)) / float64(time.Millisecond)
	log.Printf("%d queries, average %.4f ms / query",
		len(qids), dur/float64(nq))

	removeTempFileBench(b, f)
}

func BenchmarkSqliteLsh128(b *testing.B) {
	runSqlite(2, 64, 10000, 100, b)
}

func BenchmarkSqliteLsh256(b *testing.B) {
	runSqlite(4, 64, 10000, 100, b)
}

func BenchmarkSqliteLsh512(b *testing.B) {
	runSqlite(8, 64, 10000, 100, b)
}
