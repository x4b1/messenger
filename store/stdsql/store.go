package stdsql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v4/stdlib"

	"github.com/xabi93/messenger"
	"github.com/xabi93/messenger/store"
)

// errors.
var (
	ErrMissingSchemaName = errors.New("missing schema name")
)

// MessagesTable is the table name that will be used if no other table name provided.
const MessagesTable = "messages"

type config struct {
	Table  string
	Schema string
}

// Option is a function to set options to Publisher.
type Option func(*config)

// WithSchema setups schema name.
func WithSchema(s string) Option {
	return func(c *config) {
		c.Schema = s
	}
}

// WithTableName setups table name.
func WithTableName(t string) Option {
	return func(c *config) {
		c.Table = t
	}
}

// Open returns a pgx source connected to database connection string with config.
func Open(ctx context.Context, connStr string, opts ...Option) (*Store, error) {
	db, err := sql.Open("pgx", connStr)
	if err != nil {
		return nil, fmt.Errorf("postgres connect parsing conf: %w", err)
	}

	return WithInstance(ctx, db, opts...)
}

// WithInstance returns Store source initialised with the given connection instance and config.
func WithInstance(ctx context.Context, conn *sql.DB, opts ...Option) (*Store, error) {
	var err error
	if err := conn.PingContext(ctx); err != nil {
		return nil, err
	}

	var conf config
	for _, opt := range opts {
		opt(&conf)
	}

	if conf.Schema == "" {
		if conf.Schema, err = currentSchema(ctx, conn); err != nil {
			return nil, err
		}
		if conf.Schema == "" {
			return nil, ErrMissingSchemaName
		}
	}

	if conf.Table == "" {
		conf.Table = MessagesTable
	}

	px := Store{
		conn:   conn,
		config: conf,
	}

	if err := px.ensureTable(ctx); err != nil {
		return nil, err
	}

	return &px, nil
}

// Store is the instance to store and retrieve the messages in PostgreSQL database.
type Store struct {
	conn *sql.DB

	config config
}

type executor interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
}

// Store saves messages.
func (p Store) Store(ctx context.Context, tx *sql.Tx, msgs ...messenger.Message) error {
	valueStr := make([]string, len(msgs))
	totalArgs := 4
	valueArgs := make([]interface{}, 0, len(msgs)*totalArgs)
	for i, msg := range msgs {
		valueStr[i] = fmt.Sprintf("($%d, $%d, $%d, $%d)", i*totalArgs+1, i*totalArgs+2, i*totalArgs+3, i*totalArgs+4)
		valueArgs = append(valueArgs, uuid.Must(uuid.NewRandom()), metadata(msg.Metadata()), msg.Payload(), time.Now())
	}

	stmt := fmt.Sprintf(
		`INSERT INTO %q.%q (id, metadata, payload, created_at) VALUES %s`,
		p.config.Schema,
		p.config.Table,
		strings.Join(valueStr, ","),
	)

	var exec executor = p.conn
	if tx != nil {
		exec = tx
	}
	_, err := exec.ExecContext(ctx, stmt, valueArgs...)

	return err
}

// Messages returns a list of unpublished messages ordered by created at, first the oldest.
func (p Store) Messages(ctx context.Context, batch int) ([]*store.Message, error) {
	rows, err := p.conn.QueryContext(
		ctx,
		fmt.Sprintf(
			`SELECT id, metadata, payload, created_at FROM %q.%q WHERE published = false ORDER BY created_at ASC LIMIT $1`,
			p.config.Schema,
			p.config.Table,
		),
		batch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	msgs := make([]*store.Message, 0, batch)
	for rows.Next() {
		msg := &store.Message{}
		metadata := metadata(msg.Metadata)
		if err := rows.Scan(&msg.ID, &metadata, &msg.Payload, &msg.At); err != nil {
			return nil, err
		}
		msg.Metadata = metadata
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return msgs, nil
}

// Published marks as published the given messages.
func (p Store) Published(ctx context.Context, msgs ...*store.Message) error {
	ids := make([]string, len(msgs))
	for i, msg := range msgs {
		ids[i] = msg.ID
	}

	_, err := p.conn.ExecContext(
		ctx,
		fmt.Sprintf(`UPDATE %q.%q SET published = TRUE WHERE id = ANY($1)`, p.config.Schema, p.config.Table),
		ids,
	)

	return err
}

// ensureTable creates if not exists the table to store messages.
func (p *Store) ensureTable(ctx context.Context) error {
	// Check if table already exists, we cannot use `CREATE TABLE IF NOT EXISTS`,
	// maybe the user does not have permissions to CREATE and it will fail
	row := p.conn.QueryRowContext(
		ctx,
		`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`,
		p.config.Schema,
		p.config.Table,
	)

	var count int
	if err := row.Scan(&count); err != nil {
		return err
	}

	if count == 1 {
		return nil
	}

	_, err := p.conn.ExecContext(
		ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (
			id UUID PRIMARY KEY,
			metadata JSONB NOT NULL,
			payload TEXT,
			published BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
			p.config.Schema,
			p.config.Table,
		),
	)
	if err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	return nil
}

// currentSchema returns the connection schema is using.
func currentSchema(ctx context.Context, db *sql.DB) (string, error) {
	var schemaName string
	if err := db.QueryRowContext(ctx, `SELECT CURRENT_SCHEMA()`).Scan(&schemaName); err != nil {
		return "", err
	}

	return schemaName, nil
}
