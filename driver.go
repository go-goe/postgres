package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/go-goe/goe"
	"github.com/go-goe/goe/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Driver struct {
	dns string
	sql *pgxpool.Pool
	Config
}

func (d *Driver) GetDatabaseConfig() *goe.DatabaseConfig {
	return &d.Config.DatabaseConfig
}

type Config struct {
	goe.DatabaseConfig
	MigratePath string // output sql file, if defined the driver will not auto apply the migration
}

func Open(dns string, config Config) (driver *Driver) {
	return &Driver{
		dns:    dns,
		Config: config,
	}
}

func (dr *Driver) Init() error {
	config, err := pgxpool.ParseConfig(dr.dns)
	if err != nil {
		// logged by goe
		return err
	}

	dr.sql, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		// logged by goe
		return err
	}

	return dr.sql.Ping(context.Background())
}

func (dr *Driver) KeywordHandler(s string) string {
	return keywordHandler(s)
}

func keywordHandler(s string) string {
	return fmt.Sprintf(`"%s"`, s)
}

func (dr *Driver) Name() string {
	return "PostgreSQL"
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
	return Connection{sql: dr.sql, config: dr.Config}
}

type Connection struct {
	config Config
	sql    *pgxpool.Pool
}

func (c Connection) QueryContext(ctx context.Context, query *model.Query) (goe.Rows, error) {
	buildSql(query)
	rows, err := c.sql.Query(ctx, query.RawSql, query.Arguments...)
	return Rows{rows: rows}, err
}

func (c Connection) QueryRowContext(ctx context.Context, query *model.Query) goe.Row {
	buildSql(query)
	row := c.sql.QueryRow(ctx, query.RawSql, query.Arguments...)

	return Row{row: row}
}

func (c Connection) ExecContext(ctx context.Context, query *model.Query) error {
	buildSql(query)
	_, err := c.sql.Exec(ctx, query.RawSql, query.Arguments...)

	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (goe.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, convertTxOptions(opts))
	return Transaction{tx: tx, config: dr.Config}, err
}

type Transaction struct {
	config Config
	tx     pgx.Tx
}

func (t Transaction) QueryContext(ctx context.Context, query *model.Query) (goe.Rows, error) {
	buildSql(query)
	rows, err := t.tx.Query(ctx, query.RawSql, query.Arguments...)
	return Rows{rows: rows}, err
}

func (t Transaction) QueryRowContext(ctx context.Context, query *model.Query) goe.Row {
	buildSql(query)
	row := t.tx.QueryRow(ctx, query.RawSql, query.Arguments...)

	return Row{row: row}
}

func (t Transaction) ExecContext(ctx context.Context, query *model.Query) error {
	buildSql(query)
	_, err := t.tx.Exec(ctx, query.RawSql, query.Arguments...)

	return err
}

func (t Transaction) Commit() error {
	err := t.tx.Commit(context.TODO())
	if err != nil {
		// goe can't log
		return t.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func (t Transaction) Rollback() error {
	err := t.tx.Rollback(context.Background())
	if err != nil {
		// goe can't log
		return t.config.ErrorHandler(context.TODO(), err)
	}
	return nil
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
