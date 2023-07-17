package stdsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/DATA-DOG/go-txdb"
	"github.com/stretchr/testify/require"
	store "github.com/x4b1/messenger/store/postgres/stdsql"
	"github.com/x4b1/messenger/store/postgres/test"
)

var (
	dbURL string
	db    *sql.DB
)

func TestMain(m *testing.M) {
	conn, clean, err := test.Setup(context.Background())
	if err != nil {
		log.Fatal(err)
	}

	dbURL = conn.Config().ConnString()

	db, err = sql.Open("pgx", dbURL)
	if err != nil {
		log.Fatal(err.Error())
	}

	txdb.Register("txdb", "pgx", dbURL)

	code := m.Run()

	clean()

	os.Exit(code)
}

func NewTestStore(t *testing.T) (*store.Store, *sql.DB) {
	t.Helper()

	txDB, err := sql.Open("txdb", fmt.Sprintf("connection_%d", time.Now().UnixNano()))
	require.NoError(t, err)

	s, err := store.WithInstance(context.Background(), txDB)
	require.NoError(t, err)

	return s, txDB
}
