package sqllsh

import (
	"database/sql"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func creatTempFile(t *testing.T) *os.File {
	tmpfile, err := ioutil.TempFile("", "_test")
	if err != nil {
		t.Fatal(err)
	}
	return tmpfile
}

func removeTempFile(t *testing.T, tempfile *os.File) {
	if err := tempfile.Close(); err != nil {
		t.Fatal(err)
	}
}

func randomSigs(n, size int, max float64) []Signature {
	random := rand.New(rand.NewSource(1))
	sigs := make([]Signature, n)
	for i := 0; i < n; i++ {
		sigs[i] = make(Signature, size)
		for d := 0; d < size; d++ {
			sigs[i][d] = uint(random.Float64() * max)
		}
	}
	return sigs
}

func Test_NewSqliteLsh(t *testing.T) {
	f := creatTempFile(t)
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Error(err)
	}
	_, err = NewSqliteLsh(2, 2, "lshtable", db)
	if err != nil {
		t.Error(err)
	}
	removeTempFile(t, f)
}

func Test_Insert(t *testing.T) {
	f := creatTempFile(t)
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Error(err)
	}
	lsh, err := NewSqliteLsh(2, 2, "lshtable", db)
	if err != nil {
		t.Error(err)
	}
	err = lsh.Insert(1, []uint{1, 2, 3})
	if err == nil {
		t.Error("Fail to raise error")
	}
	err = lsh.Insert(1, []uint{0, 1, 2, 3})
	if err != nil {
		t.Error(err)
	}
	removeTempFile(t, f)
}

func Test_Query(t *testing.T) {
	f := creatTempFile(t)
	db, err := sql.Open("sqlite3", f.Name())
	if err != nil {
		t.Error(err)
	}
	lsh, err := NewSqliteLsh(2, 2, "lshtable", db)
	if err != nil {
		t.Error(err)
	}
	sigs := randomSigs(10, 4, math.MaxFloat64)
	for i := range sigs {
		lsh.Insert(i, sigs[i])
	}
	lsh.Index()
	if err != nil {
		t.Error(err)
	}
	for i := range sigs {
		out := make(chan int)
		go func(sig Signature) {
			err := lsh.Query(sig, out)
			if err != nil {
				t.Error(err)
			}
			close(out)
		}(sigs[i])
		found := false
		for id := range out {
			if id == i {
				found = true
			}
		}
		if !found {
			t.Error("Error in query")
		}
	}
	removeTempFile(t, f)
}
