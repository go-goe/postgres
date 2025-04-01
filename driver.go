package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/olauro/goe"
	"github.com/olauro/goe/model"
)

type Driver struct {
	dns    string
	sql    *pgxpool.Pool
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
	config, err := pgxpool.ParseConfig(dr.dns)
	if err != nil {
		return err
	}

	dr.sql, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		return err
	}

	err = dr.sql.Ping(context.Background())
	if err != nil {
		return err
	}

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

func (dr *Driver) Log(b bool) {
	dr.config.LogQuery = b
}

func (dr *Driver) Stats() sql.DBStats {
	stat := dr.sql.Stat()
	return sql.DBStats{
		MaxOpenConnections: int(stat.MaxConns()),           // Max connections allowed
		OpenConnections:    int(stat.AcquiredConns()),      // Currently acquired (open) connections
		InUse:              int(stat.AcquiredConns()),      // Connections currently in use
		Idle:               int(stat.IdleConns()),          // Connections in idle state
		WaitCount:          stat.AcquireCount(),            // Total successful connection acquisitions
		WaitDuration:       stat.AcquireDuration(),         // Time spent waiting for a connection
		MaxIdleClosed:      stat.MaxIdleDestroyCount(),     // Connections closed due to idle timeout
		MaxLifetimeClosed:  stat.MaxLifetimeDestroyCount(), // Connections closed due to max lifetime
	}
}

func (dr *Driver) Close() error {
	dr.sql.Close()
	return nil
}

func (dr *Driver) NewConnection() goe.Connection {
	return Connection{sql: dr.sql, config: dr.config}
}

type Connection struct {
	config Config
	sql    *pgxpool.Pool
}

func (c Connection) QueryContext(ctx context.Context, query model.Query) (goe.Rows, error) {
	rows, err := c.sql.Query(ctx, buildSql(&query, c.config.LogQuery), query.Arguments...)
	if err != nil {
		return nil, err
	}

	return Rows{rows: rows}, nil
}

func (c Connection) QueryRowContext(ctx context.Context, query model.Query) goe.Row {
	row := c.sql.QueryRow(ctx, buildSql(&query, c.config.LogQuery), query.Arguments...)

	return Row{row: row}
}

func (c Connection) ExecContext(ctx context.Context, query model.Query) error {
	_, err := c.sql.Exec(ctx, buildSql(&query, c.config.LogQuery), query.Arguments...)

	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (goe.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, convertTxOptions(opts))
	if err != nil {
		return nil, err
	}
	return Transaction{tx: tx, config: dr.config}, nil
}

type Transaction struct {
	config Config
	tx     pgx.Tx
}

func (t Transaction) QueryContext(ctx context.Context, query model.Query) (goe.Rows, error) {
	rows, err := t.tx.Query(ctx, buildSql(&query, t.config.LogQuery), query.Arguments...)
	if err != nil {
		return nil, err
	}

	return Rows{rows: rows}, nil
}

func (t Transaction) QueryRowContext(ctx context.Context, query model.Query) goe.Row {
	row := t.tx.QueryRow(ctx, buildSql(&query, t.config.LogQuery), query.Arguments...)

	return Row{row: row}
}

func (t Transaction) ExecContext(ctx context.Context, query model.Query) error {
	_, err := t.tx.Exec(ctx, buildSql(&query, t.config.LogQuery), query.Arguments...)

	return err
}

func (t Transaction) Commit() error {
	return t.tx.Commit(context.Background())
}

func (t Transaction) Rollback() error {
	return t.tx.Rollback(context.Background())
}

type Rows struct {
	rows pgx.Rows
}

func (rs Rows) Close() error {
	rs.rows.Close()
	return nil
}

func (rs Rows) Next() bool {
	return rs.rows.Next()
}

func (rs Rows) Scan(dest ...any) error {
	return rs.rows.Scan(dest...)
}

type Row struct {
	row pgx.Row
}

func (r Row) Scan(dest ...any) error {
	return r.row.Scan(dest...)
}

func convertTxOptions(sqlOpts *sql.TxOptions) pgx.TxOptions {
	var isoLevel pgx.TxIsoLevel

	switch sqlOpts.Isolation {
	case sql.LevelDefault:
		isoLevel = pgx.ReadCommitted // Default for PostgreSQL
	case sql.LevelReadUncommitted:
		isoLevel = pgx.ReadUncommitted
	case sql.LevelReadCommitted:
		isoLevel = pgx.ReadCommitted
	case sql.LevelRepeatableRead:
		isoLevel = pgx.RepeatableRead
	case sql.LevelSerializable:
		isoLevel = pgx.Serializable
	default:
		isoLevel = pgx.Serializable
	}

	return pgx.TxOptions{
		IsoLevel: isoLevel,
		AccessMode: func() pgx.TxAccessMode {
			if sqlOpts.ReadOnly {
				return pgx.ReadOnly
			}
			return pgx.ReadWrite
		}(),
	}
}
