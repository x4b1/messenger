package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/x4b1/messenger"

	"github.com/google/uuid"
)

// errors.
var (
	ErrMissingSchemaName = errors.New("missing schema name")
)

// DefaultMessagesTable is the table name that will be used if no other table name provided.
const DefaultMessagesTable = "messages"

// WithInstance returns Store source initialised with the given connection instance and config.
func New(ctx context.Context, db Instance, opts ...Option) (*Storer, error) {
	if err := db.Ping(ctx); err != nil {
		return nil, err
	}

	s := Storer{db: db}

	for _, opt := range opts {
		opt(&s)
	}

	var err error
	if s.schema == "" {
		if s.schema, err = currentSchema(ctx, db); err != nil {
			return nil, err
		}
		if s.schema == "" {
			return nil, ErrMissingSchemaName
		}
	}

	if s.table == "" {
		s.table = DefaultMessagesTable
	}

	if err := s.ensureTable(ctx); err != nil {
		return nil, err
	}

	return &s, nil
}

type Storer struct {
	db Instance

	schema string
	table  string
}

// Store saves messages.
func (s *Storer) Store(ctx context.Context, tx Executor, msgs ...messenger.Message) error {
	valueStr := make([]string, len(msgs))
	totalArgs := 4
	valueArgs := make([]any, 0, len(msgs)*totalArgs)
	for i, msg := range msgs {
		valueStr[i] = fmt.Sprintf("($%d, $%d, $%d, $%d)", i*totalArgs+1, i*totalArgs+2, i*totalArgs+3, i*totalArgs+4)
		valueArgs = append(valueArgs, uuid.Must(uuid.NewRandom()), metadata(msg.GetMetadata()), msg.GetPayload(), time.Now())
	}

	stmt := fmt.Sprintf(
		`INSERT INTO %q.%q (id, metadata, payload, created_at) VALUES %s`,
		s.schema,
		s.table,
		strings.Join(valueStr, ","),
	)

	var exec Executor = s.db
	if tx != nil {
		exec = tx
	}

	if err := exec.Exec(ctx, stmt, valueArgs...); err != nil {
		return fmt.Errorf("storing messages: %w", err)
	}

	return nil
}

// Messages returns a list of unpublished messages ordered by created at, first the oldest.
func (s Storer) Messages(ctx context.Context, batch int) ([]messenger.Message, error) {
	rows, err := s.db.Query(
		ctx,
		fmt.Sprintf(
			`SELECT id, metadata, payload, created_at FROM %q.%q WHERE published = false ORDER BY created_at ASC LIMIT $1`,
			s.schema,
			s.table,
		),
		batch,
	)
	if err != nil {
		return nil, fmt.Errorf("getting messages: %w", err)
	}
	defer rows.Close()

	msgs := make([]messenger.Message, 0, batch)
	for rows.Next() {
		msg := &messenger.GenericMessage{}
		md := metadata(msg.Metadata)
		if err := rows.Scan(&msg.Id, &md, &msg.Payload, &msg.At); err != nil {
			return nil, fmt.Errorf("scanning message: %w", err)
		}
		msg.Metadata = md
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// Published marks as published the given messages.
func (s Storer) Published(ctx context.Context, msgs ...messenger.Message) error {
	ids := make([]string, len(msgs))
	for i, msg := range msgs {
		ids[i] = msg.ID()
	}

	if err := s.db.Exec(ctx, fmt.Sprintf(`UPDATE %q.%q SET published = TRUE WHERE id = ANY($1)`, s.schema, s.table), ids); err != nil {
		return fmt.Errorf("updating published messages: %w", err)
	}

	return nil
}

// ensureTable creates if not exists the table to store messages.
func (s *Storer) ensureTable(ctx context.Context) error {
	// Check if table already exists, we cannot use `CREATE TABLE IF NOT EXISTS`,
	// maybe the user does not have permissions to CREATE and it will fail
	row := s.db.QueryRow(
		ctx,
		`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`,
		s.schema,
		s.table,
	)

	var count int
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("ensuring outbox table exists: %w", err)
	}

	if count == 1 {
		return nil
	}

	err := s.db.Exec(
		ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (
			id UUID PRIMARY KEY,
			metadata JSONB NOT NULL,
			payload TEXT,
			published BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
			s.schema,
			s.table,
		),
	)
	if err != nil {
		return fmt.Errorf("creating table: %w", err)
	}

	return nil
}

// currentSchema returns the connection schema is using.
func currentSchema(ctx context.Context, db Instance) (string, error) {
	var schemaName string
	if err := db.QueryRow(ctx, `SELECT CURRENT_SCHEMA()`).Scan(&schemaName); err != nil {
		return "", fmt.Errorf("getting current schema: %w", err)
	}

	return schemaName, nil
}

func (s *Storer) DeletePublishedByExpiration(ctx context.Context, d time.Duration) error {
	err := s.db.Exec(
		ctx,
		fmt.Sprintf("DELETE FROM %q.%q WHERE published = TRUE AND created_at < $1;", s.schema, s.table),
		time.Now().Add(-d),
	)
	if err != nil {
		return fmt.Errorf("deleting published messages: %w", err)
	}

	return nil
}