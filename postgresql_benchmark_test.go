package sqllsh

import (
	"database/sql"
	"log"
	"math"
	"math/rand"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func conn() (*sql.DB, error) {
	return sql.Open("postgres", "")
}

func runPostgres(k, l, n, nq int, b *testing.B) {
	// Initialize database
	db, err := conn()
	if err != nil {
		b.Fatal(err)
	}
	_, err = db.Exec("DROP TABLE IF EXISTS lshtable;")
	if err != nil {
		b.Fatal(err)
	}

	// Initialize data
	lsh, err := NewPostgresLsh(k, l, "lshtable", db)
	if err != nil {
		b.Fatal(err)
	}
	sigs := randomSigs(n, k*l, math.MaxFloat64)
	ids := make([]int, len(sigs))
	for i := range sigs {
		ids[i] = i
	}
	qids := rand.Perm(len(ids))[:nq]
	b.ResetTimer()

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

	// Clean up
	_, err = db.Exec("DROP TABLE IF EXISTS lshtable;")
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPostgresLsh128(b *testing.B) {
	runPostgres(2, 64, 10000, 100, b)
}

func BenchmarkPostgresLsh256(b *testing.B) {
	runPostgres(4, 64, 10000, 100, b)
}

func BenchmarkPostgresLsh512(b *testing.B) {
	runPostgres(8, 64, 10000, 100, b)
}
