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

func runInsertPostgres(k, l, n int, b *testing.B) {
	sigs := randomSigs(n, k*l, math.MaxFloat64)
	db, err := conn()
	if err != nil {
		b.Fatal(err)
	}
	// Delete table if exist
	_, err = db.Exec("DROP TABLE IF EXISTS lshtable;")
	if err != nil {
		b.Fatal(err)
	}
	lsh, err := NewPostgresLsh(k, l, "lshtable", db)
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
	if err != nil {
		b.Fatal(err)
	}
	dur = float64(time.Now().Sub(start)) / float64(time.Second)
	log.Printf("Building index takes %.4f seconds", dur)

	// Clean up
	_, err = db.Exec("DROP TABLE IF EXISTS lshtable;")
	if err != nil {
		b.Fatal(err)
	}
}

func runQueryPostgres(k, l, n, nq int, b *testing.B) {
	sigs := randomSigs(n, k*l, math.MaxFloat64)
	db, err := conn()
	if err != nil {
		b.Fatal(err)
	}
	// Delete table if exist
	_, err = db.Exec("DROP TABLE IF EXISTS lshtable;")
	if err != nil {
		b.Fatal(err)
	}
	lsh, err := NewPostgresLsh(k, l, "lshtable", db)
	if err != nil {
		b.Fatal(err)
	}
	ids := make([]int, len(sigs))
	for i := range sigs {
		ids[i] = i
	}
	qids := rand.Perm(len(ids))[:nq]

	start := time.Now()
	err = lsh.BatchInsert(ids, sigs)
	if err != nil {
		b.Fatal(err)
	}
	dur := float64(time.Now().Sub(start)) / float64(time.Second)
	log.Printf("Batch inserting %d signatures takes %.4f seconds", len(sigs), dur)
	start = time.Now()
	lsh.Index()
	if err != nil {
		b.Fatal(err)
	}
	dur = float64(time.Now().Sub(start)) / float64(time.Second)
	log.Printf("Building index takes %.4f seconds", dur)

	b.ResetTimer()

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
	dur = float64(time.Now().Sub(start)) / float64(time.Second)
	log.Printf("%d queries takes %.4f seconds", len(qids), dur)

	// Clean up
	_, err = db.Exec("DROP TABLE IF EXISTS lshtable;")
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPostgresLshInsert128(b *testing.B) {
	runInsertPostgres(2, 64, 10000, b)
}

func BenchmarkPostgresLshInsert256(b *testing.B) {
	runInsertPostgres(4, 64, 10000, b)
}

func BenchmarkPostgresLshInsert512(b *testing.B) {
	runInsertPostgres(8, 64, 10000, b)
}

func BenchmarkPostgresLshQuery128(b *testing.B) {
	runQueryPostgres(2, 64, 10000, 1000, b)
}
