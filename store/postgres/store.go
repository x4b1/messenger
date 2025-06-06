package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/x4b1/messenger"
	"github.com/x4b1/messenger/inspect"
	"github.com/x4b1/messenger/store"
)

// errors.
var (
	ErrMissingSchemaName = errors.New("missing schema name")
)

// DefaultMessagesTable is the table name that will be used if no other table name provided.
const DefaultMessagesTable = "messages"

// New returns a postgres store initialised with the given connection instance and config.
func New[T any](ctx context.Context, db Instance, opts ...Option) (*Storer[T], error) {
	if err := db.Ping(ctx); err != nil {
		return nil, err
	}

	s := Storer[T]{
		db:          db,
		transformer: store.DefaultTransformer[T](),
	}

	for _, opt := range opts {
		opt(&s)
	}
	for _, opt := range opts {
		opt(&s.config)
	}

	var err error
	if s.config.schema == "" {
		if s.config.schema, err = currentSchema(ctx, db); err != nil {
			return nil, err
		}
		if s.config.schema == "" {
			return nil, ErrMissingSchemaName
		}
	}

	if s.config.table == "" {
		s.config.table = DefaultMessagesTable
	}

	if err := s.ensureTable(ctx); err != nil {
		return nil, err
	}

	return &s, nil
}

type config struct {
	schema      string
	table       string
	jsonPayload bool
}

// Storer is the implementation of messages store for postgres.
type Storer[T any] struct {
	db Instance

	config config

	transformer store.Transformer[T]
}

// Store saves messages.
func (s *Storer[T]) Store(ctx context.Context, tx Executor, msgs ...T) error {
	if len(msgs) == 0 {
		// if no messages to store provided do nothing.
		return nil
	}
	valueStr := make([]string, len(msgs))
	totalArgs := 5
	valueArgs := make([]any, 0, len(msgs)*totalArgs)
	for i, inMsg := range msgs {
		msg, err := s.transformer.Transform(ctx, inMsg)
		if err != nil {
			return fmt.Errorf("transforming message before store: %w", err)
		}
		//nolint: mnd // need it to point to each argument to insert
		valueStr[i] = fmt.Sprintf(
			"($%d, $%d, $%d, $%d, $%d)",
			i*totalArgs+1, i*totalArgs+2, i*totalArgs+3, i*totalArgs+4, i*totalArgs+5)
		valueArgs = append(
			valueArgs,
			msg.ID(), msg.Metadata(), msg.Payload(), msg.Published(), msg.At().UTC(),
		)
	}

	stmt := fmt.Sprintf(
		`INSERT INTO %q.%q (id, metadata, payload, published, created_at) VALUES %s`,
		s.config.schema,
		s.config.table,
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
func (s Storer[T]) Messages(ctx context.Context, batch int) ([]messenger.Message, error) {
	rows, err := s.db.Query(
		ctx,
		fmt.Sprintf(
			`SELECT
				id, metadata, payload, published, created_at
			FROM
				%q.%q
			WHERE
				published = false
			ORDER BY
				created_at ASC
			LIMIT $1`,
			s.config.schema,
			s.config.table,
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
		if err := rows.Scan(&msg.MsgID, &msg.MsgMetadata, &msg.MsgPayload, &msg.MsgPublished, &msg.MsgAt); err != nil {
			return nil, fmt.Errorf("scanning message: %w", err)
		}
		msgs = append(msgs, msg)
	}

	return msgs, nil
}

// Published marks as published the given messages.
func (s Storer[T]) Published(ctx context.Context, msg messenger.Message) error {
	if err := s.db.Exec(ctx,
		fmt.Sprintf(`UPDATE %q.%q SET published = TRUE WHERE id = $1`, s.config.schema, s.config.table),
		msg.ID(),
	); err != nil {
		return fmt.Errorf("updating published messages: %w", err)
	}

	return nil
}

// Find returns a list of paginated messages filtered by the given query.
func (s Storer[T]) Find(ctx context.Context, q *inspect.Query) (*inspect.Result, error) {
	rows, err := s.db.Query(
		ctx,
		fmt.Sprintf(
			`SELECT id, metadata, payload, published, created_at FROM %q.%q ORDER BY created_at DESC LIMIT $1 OFFSET $2`,
			s.config.schema,
			s.config.table,
		),
		q.Limit,
		q.Limit*(q.Page-1),
	)
	if err != nil {
		return nil, fmt.Errorf("getting messages: %w", err)
	}
	defer rows.Close()

	result := inspect.Result{
		Msgs: make([]*messenger.GenericMessage, 0),
	}
	for rows.Next() {
		msg := &messenger.GenericMessage{}
		if err := rows.Scan(&msg.MsgID, &msg.MsgMetadata, &msg.MsgPayload, &msg.MsgPublished, &msg.MsgAt); err != nil {
			return nil, fmt.Errorf("scanning message: %w", err)
		}
		result.Msgs = append(result.Msgs, msg)
	}

	if result.Total, err = s.count(ctx, q); err != nil {
		return nil, err
	}

	return &result, nil
}

func (s Storer[T]) count(ctx context.Context, _ *inspect.Query) (int, error) {
	var count int
	err := s.db.QueryRow(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %q.%q", s.config.schema, s.config.table)).
		Scan(&count)
	if err != nil {
		return count, fmt.Errorf("counting messages: %w", err)
	}

	return count, nil
}

// ensureTable creates if not exists the table to store messages.
func (s *Storer[T]) ensureTable(ctx context.Context) error {
	// Check if table already exists, we cannot use `CREATE TABLE IF NOT EXISTS`,
	// maybe the user does not have permissions to CREATE and it will fail
	row := s.db.QueryRow(
		ctx,
		`SELECT COUNT(1) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2 LIMIT 1`,
		s.config.schema,
		s.config.table,
	)

	var count int
	if err := row.Scan(&count); err != nil {
		return fmt.Errorf("ensuring outbox table exists: %w", err)
	}

	if count == 1 {
		return nil
	}

	payloadType := "TEXT"
	if s.config.jsonPayload {
		payloadType = "JSONB"
	}

	err := s.db.Exec(
		ctx,
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (
			id UUID PRIMARY KEY,
			metadata JSONB NOT NULL,
			payload %s,
			published BOOLEAN DEFAULT FALSE,
			created_at TIMESTAMP NOT NULL DEFAULT NOW()
		)`,
			s.config.schema,
			s.config.table,
			payloadType,
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

// DeletePublishedByExpiration performs a hard delete of the messages with the column published to true
// and created at lower than the given duration.
func (s *Storer[T]) DeletePublishedByExpiration(ctx context.Context, d time.Duration) error {
	err := s.db.Exec(
		ctx,
		fmt.Sprintf(
			"DELETE FROM %q.%q WHERE published = TRUE AND created_at < $1;",
			s.config.schema,
			s.config.table,
		),
		time.Now().UTC().Add(-d),
	)
	if err != nil {
		return fmt.Errorf("deleting published messages: %w", err)
	}

	return nil
}

// Republish given a list of message ids set published to FALSE.
// If the given message id does not exists it skips.
func (s *Storer[T]) Republish(ctx context.Context, msgID ...string) error {
	err := s.db.Exec(
		ctx,
		fmt.Sprintf(
			`UPDATE %q.%q SET published = FALSE WHERE id = ANY($1)`,
			s.config.schema,
			s.config.table,
		),
		msgID,
	)
	if err != nil {
		return fmt.Errorf("republishing published messages: %w", err)
	}

	return nil
}
