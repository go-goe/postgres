package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/olauro/goe"
	"github.com/olauro/goe/model"
)

type Driver struct {
	dns    string
	sql    *sql.DB
	config Config
}

type Config struct {
	LogQuery bool
}

func Open(dns string, config Config) (driver *Driver) {
	return &Driver{
		dns:    dns,
		config: config,
	}
}

func (dr *Driver) Init() error {
	config, err := pgx.ParseConfig(dr.dns)
	if err != nil {
		return err
	}
	dr.sql = stdlib.OpenDB(*config)
	return nil
}

func (dr *Driver) KeywordHandler(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

func keywordHandler(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

func (dr *Driver) Name() string {
	return "PostgreSQL"
}

func (dr *Driver) Stats() sql.DBStats {
	return dr.sql.Stats()
}

func (dr *Driver) Close() error {
	return dr.sql.Close()
}

func (dr *Driver) NewConnection() goe.Connection {
	return Connection{sql: dr.sql, config: dr.config}
}

type Connection struct {
	config Config
	sql    *sql.DB
}

func (c Connection) QueryContext(ctx context.Context, query model.Query) (goe.Rows, error) {
	rows, err := c.sql.QueryContext(ctx, buildSql(&query, c.config.LogQuery), query.Arguments...)
	if err != nil {
		return nil, err
	}

	return Rows{rows: rows}, nil
}

func (c Connection) QueryRowContext(ctx context.Context, query model.Query) goe.Row {
	row := c.sql.QueryRowContext(ctx, buildSql(&query, c.config.LogQuery), query.Arguments...)

	return Row{row: row}
}

func (c Connection) ExecContext(ctx context.Context, query model.Query) error {
	_, err := c.sql.ExecContext(ctx, buildSql(&query, c.config.LogQuery), query.Arguments...)

	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (goe.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return Transaction{tx: tx, config: dr.config}, nil
}

type Transaction struct {
	config Config
	tx     *sql.Tx
}

func (t Transaction) QueryContext(ctx context.Context, query model.Query) (goe.Rows, error) {
	rows, err := t.tx.QueryContext(ctx, buildSql(&query, t.config.LogQuery), query.Arguments...)
	if err != nil {
		return nil, err
	}

	return Rows{rows: rows}, nil
}

func (t Transaction) QueryRowContext(ctx context.Context, query model.Query) goe.Row {
	row := t.tx.QueryRowContext(ctx, buildSql(&query, t.config.LogQuery), query.Arguments...)

	return Row{row: row}
}

func (t Transaction) ExecContext(ctx context.Context, query model.Query) error {
	_, err := t.tx.ExecContext(ctx, buildSql(&query, t.config.LogQuery), query.Arguments...)

	return err
}

func (t Transaction) Commit() error {
	return t.tx.Commit()
}

func (t Transaction) Rollback() error {
	return t.tx.Rollback()
}

type Rows struct {
	rows *sql.Rows
}

func (rs Rows) Close() error {
	return rs.rows.Close()
}

func (rs Rows) Next() bool {
	return rs.rows.Next()
}

func (rs Rows) Scan(dest ...any) error {
	return rs.rows.Scan(dest...)
}

type Row struct {
	row *sql.Row
}

func (r Row) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}
