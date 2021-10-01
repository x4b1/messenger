package pgx

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgconn"
	"github.com/jackc/pgx/v4"

	"github.com/xabi93/messenger"
)

// errors.
var (
	ErrMissingSchemaName = errors.New("missing schema name")
)

// MessagesTable is the table name that will be used if no other table name provided.
const MessagesTable = "messages"

type Config struct {
	Table  string
	Schema string
}

// Open returns a Postgres source connected to database connection string with config.
func Open(ctx context.Context, connStr string, conf Config) (*Postgres, error) {
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return nil, err
	}

	return WithInstance(ctx, conn, conf)
}

// WithInstance returns Postgres source initialized with the given connection instance and config.
func WithInstance(ctx context.Context, conn *pgx.Conn, config Config) (*Postgres, error) {
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

	if config.Table == "" {
		config.Table = MessagesTable
	}

	px := Postgres{
		conn:   conn,
		config: config,
	}

	if err := px.ensureTable(ctx); err != nil {
		return nil, err
	}

	return &px, nil
}

// Postgres is the instance to store and retrieve the messages in PostgreSQL database.
type Postgres struct {
	conn *pgx.Conn

	config Config
}

type executor interface {
	Exec(context.Context, string, ...interface{}) (pgconn.CommandTag, error)
}

// Store saves messages.
func (p Postgres) Store(ctx context.Context, tx pgx.Tx, msgs ...*messenger.Message) error {
	valueStr := make([]string, len(msgs))
	totalArgs := 4
	valueArgs := make([]interface{}, 0, len(msgs)*totalArgs)
	for i, msg := range msgs {
		valueStr[i] = fmt.Sprintf("($%d, $%d, $%d, $%d)", i*totalArgs+1, i*totalArgs+2, i*totalArgs+3, i*totalArgs+4)
		valueArgs = append(valueArgs, msg.ID, msg.Metadata, msg.Payload, msg.CreatedAt)
	}

	stmt := fmt.Sprintf(`INSERT INTO "%s"."%s" (id, metadata, payload, created_at) VALUES %s`, p.config.Schema, p.config.Table, strings.Join(valueStr, ","))

	var exec executor = p.conn
	if tx != nil {
		exec = tx
	}
	_, err := exec.Exec(ctx, stmt, valueArgs...)

	return err
}

// Messages returns a list of unpublished messages ordered by created at, first the oldest.
func (p Postgres) Messages(ctx context.Context, batch int) ([]*messenger.Message, error) {
	rows, err := p.conn.Query(
		ctx,
		fmt.Sprintf(
			`SELECT id, metadata, payload, created_at FROM "%s"."%s" WHERE published = false ORDER BY created_at ASC LIMIT $1`,
			p.config.Schema,
			p.config.Table,
		),
		batch,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	msgs := make([]*messenger.Message, 0, batch)
	for rows.Next() {
		msg := &messenger.Message{}
		if err := rows.Scan(&msg.ID, &msg.Metadata, &msg.Payload, &msg.CreatedAt); err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return msgs, nil
}

// Published marks as published the given messages.
func (p Postgres) Published(ctx context.Context, msgs ...*messenger.Message) error {
	ids := make([]uuid.UUID, len(msgs))
	for i, msg := range msgs {
		ids[i] = msg.ID
	}

	_, err := p.conn.Exec(
		ctx,
		fmt.Sprintf(`UPDATE "%s"."%s" SET published = TRUE WHERE id = ANY($1)`, p.config.Schema, p.config.Table),
		ids,
	)

	return err
}

// ensureTable creates if not exists the table to store messages.
func (p *Postgres) ensureTable(ctx context.Context) error {
	// Check if table already exists, we cannot use `CREATE TABLE IF NOT EXISTS`,
	// maybe the user does not have permissions to CREATE and it will fail
	row := p.conn.QueryRow(
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

	_, err := p.conn.Exec(
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
func currentSchema(ctx context.Context, db *pgx.Conn) (string, error) {
	var schemaName string
	if err := db.QueryRow(ctx, `SELECT CURRENT_SCHEMA()`).Scan(&schemaName); err != nil {
		return "", err
	}

	return schemaName, nil
}
