package pgx

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/publish"
	"github.com/xabi93/messenger/store"
)

// errors.
var (
	ErrMissingSchemaName = errors.New("missing schema name")
)

// MessagesTable is the table name that will be used if no other table name provided.
const (
	// MessagesTable is the default table name that will be used if no other table name provided.
	MessagesTable = "messages"
	// PublishStatusTable is the default table name to store last message published
	PublishStatusTable = "publish_status"
)

// Config defines the configuration of the pgx source
// where are going to be defined needed structures.
type Config struct {
	MessagesTable      string
	PublishStatusTable string
	Schema             string
}

// Open returns a pgx source connected to database connection string with config.
func Open(ctx context.Context, connStr string, conf Config) (*Store, error) {
	conn, err := pgxpool.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}

	return WithInstance(ctx, conn, conf)
}

// Conn defines the pgx interface to be used by the store.
type Conn interface {
	executor

	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
	Ping(ctx context.Context) error
	Query(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
}

// WithInstance returns Store source initialised with the given connection instance and config.
func WithInstance(ctx context.Context, conn Conn, config Config) (*Store, error) {
	var err error
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}

	if config.Schema == "" {
		if config.Schema, err = currentSchema(ctx, conn); err != nil {
			return nil, err
		}
		if config.Schema == "" {
			return nil, ErrMissingSchemaName
		}
	}

	if config.MessagesTable == "" {
		config.MessagesTable = MessagesTable
	}

	if config.PublishStatusTable == "" {
		config.PublishStatusTable = PublishStatusTable
	}

	px := Store{
		conn:   conn,
		config: config,
	}

	if err := px.ensureTables(ctx); err != nil {
		return nil, err
	}

	return &px, nil
}

var _ publish.Source = (*Store)(nil)

// Store is the instance to store and retrieve the messages in PostgreSQL database.
type Store struct {
	conn Conn

	config Config
}

type executor interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

// Store saves messages.
func (p Store) Store(ctx context.Context, tx pgx.Tx, msgs ...messenger.Message) error {
	valueStr := make([]string, len(msgs))
	totalArgs := 4
	valueArgs := make([]interface{}, 0, len(msgs)*totalArgs)
	for i, msg := range msgs {
		valueStr[i] = fmt.Sprintf("($%d, $%d, $%d, $%d)", i*totalArgs+1, i*totalArgs+2, i*totalArgs+3, i*totalArgs+4)
		valueArgs = append(valueArgs, uuid.Must(uuid.NewRandom()), msg.Metadata(), msg.Payload(), time.Now())
	}

	stmt := fmt.Sprintf(
		`INSERT INTO %q.%q (id, metadata, payload, created_at) VALUES %s`,
		p.config.Schema,
		p.config.MessagesTable,
		strings.Join(valueStr, ","),
	)

	var exec executor = p.conn
	if tx != nil {
		exec = tx
	}
	_, err := exec.Exec(ctx, stmt, valueArgs...)

	return err
}

// Messages returns a list of unpublished messages ordered by created at, first the oldest.
func (p Store) Messages(ctx context.Context, publisher string, batch int) ([]*store.Message, error) {
	row := p.conn.QueryRow(
		ctx,
		fmt.Sprintf(
			`SELECT last_published FROM %q.%q WHERE publisher = $1`,
			p.config.Schema,
			p.config.PublishStatusTable,
		),
		publisher,
	)

	var lastUpdated time.Time
	if err := row.Scan(&lastUpdated); err != nil {
		if !errors.Is(err, pgx.ErrNoRows) {
			return nil, fmt.Errorf("getting last published: %w", err)
		}
	}

	rows, err := p.conn.Query(
		ctx,
		fmt.Sprintf(
			`SELECT id, metadata, payload, created_at FROM %q.%q WHERE created_at > $1 ORDER BY created_at ASC LIMIT $2`,
			p.config.Schema,
			p.config.MessagesTable,
		),
		lastUpdated,
		batch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	msgs := make([]*store.Message, 0, batch)
	for rows.Next() {
		msg := &store.Message{}
		if err := rows.Scan(&msg.ID, &msg.Metadata, &msg.Payload, &msg.At); err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return msgs, nil
}

// SaveLastPublished stores in published status table the last published message created at
func (p Store) SaveLastPublished(ctx context.Context, publisherName string, msg *store.Message) error {
	_, err := p.conn.Exec(
		ctx,
		fmt.Sprintf(`INSERT INTO
		%q.%q (publisher, last_published)
		VALUES
			($1, $2)
		ON CONFLICT
			(publisher)
		DO UPDATE
			SET
				last_published = EXCLUDED.last_published`, p.config.Schema, p.config.PublishStatusTable),
		publisherName,
		msg.At,
	)

	return err
}

// ensureTable creates if not exists the table to store messages.
func (p *Store) ensureTables(ctx context.Context) error {
	// Check if table already exists, we cannot use `CREATE TABLE IF NOT EXISTS`,
	// maybe the user does not have permissions to CREATE and it will fail
	row := p.conn.QueryRow(
		ctx,
		`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name IN($2, $3) LIMIT 1`,
		p.config.Schema,
		p.config.MessagesTable,
		p.config.PublishStatusTable,
	)

	var count int
	if err := row.Scan(&count); err != nil {
		return err
	}

	//nolint: gomnd // number of tables to be created, only used here
	if count == 2 {
		return nil
	}

	tx, err := p.conn.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(
		ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (
			id UUID PRIMARY KEY,
			metadata JSONB NOT NULL,
			payload TEXT,
			published BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
			p.config.Schema,
			p.config.MessagesTable,
		),
	); err != nil {
		return fmt.Errorf("creating %q table: %w", p.config.MessagesTable, err)
	}

	if _, err := tx.Exec(
		ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (
			publisher TEXT PRIMARY KEY,
			last_published TIMESTAMP
		)`,
			p.config.Schema,
			p.config.PublishStatusTable,
		),
	); err != nil {
		return fmt.Errorf("creating %q table: %w", p.config.PublishStatusTable, err)
	}

	return tx.Commit(ctx)
}

// currentSchema returns the connection schema is using.
func currentSchema(ctx context.Context, db Conn) (string, error) {
	var schemaName string
	if err := db.QueryRow(ctx, `SELECT CURRENT_SCHEMA()`).Scan(&schemaName); err != nil {
		return "", err
	}

	return schemaName, nil
}
