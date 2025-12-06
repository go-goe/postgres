package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/go-goe/goe"
	"github.com/go-goe/goe/model"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Driver struct {
	dns string
	sql *pgxpool.Pool
	config
}

func (d *Driver) GetDatabaseConfig() *model.DatabaseConfig {
	return &d.config.DatabaseConfig
}

type config struct {
	model.DatabaseConfig
	MigratePath string
}

type Config struct {
	Logger           model.Logger
	IncludeArguments bool          // include all arguments used on query
	QueryThreshold   time.Duration // query threshold to warning on slow queries

	MigratePath string // output sql file, if defined the driver will not auto apply the migration.
}

func NewConfig(c Config) config {
	return config{
		DatabaseConfig: model.DatabaseConfig{
			Logger:           c.Logger,
			IncludeArguments: c.IncludeArguments,
			QueryThreshold:   c.QueryThreshold,
		},
		MigratePath: c.MigratePath,
	}
}

func Open(dns string, c config) (driver *Driver) {
	return &Driver{
		dns:    dns,
		config: c,
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

var errMap = map[string][]error{
	"23505": {goe.ErrBadRequest, goe.ErrUniqueValue},
	"23503": {goe.ErrBadRequest, goe.ErrForeignKey},
}

type wrapErrors struct {
	msg  string
	errs []error
}

func (e *wrapErrors) Error() string {
	return "goe: " + e.msg
}

func (e *wrapErrors) Unwrap() []error {
	return e.errs
}

func (dr *Driver) ErrorTranslator() func(err error) error {
	return func(err error) error {
		if pgError, ok := err.(*pgconn.PgError); ok {
			return &wrapErrors{msg: err.Error(), errs: append(errMap[pgError.Code], err)}
		}
		return err
	}
}

func (dr *Driver) NewConnection() model.Connection {
	return Connection{sql: dr.sql, config: dr.config}
}

type Connection struct {
	config config
	sql    *pgxpool.Pool
}

func (c Connection) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	buildSql(query)
	rows, err := c.sql.Query(ctx, query.RawSql, query.Arguments...)
	return Rows{rows: rows}, err
}

func (c Connection) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
	buildSql(query)
	row := c.sql.QueryRow(ctx, query.RawSql, query.Arguments...)

	return Row{row: row}
}

func (c Connection) ExecContext(ctx context.Context, query *model.Query) error {
	buildSql(query)
	_, err := c.sql.Exec(ctx, query.RawSql, query.Arguments...)

	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (model.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, convertTxOptions(opts))
	return Transaction{tx: tx, config: dr.config}, err
}

type Transaction struct {
	config config
	tx     pgx.Tx
	saves  int64
}

func (t Transaction) QueryContext(ctx context.Context, query *model.Query) (model.Rows, error) {
	buildSql(query)
	rows, err := t.tx.Query(ctx, query.RawSql, query.Arguments...)
	return Rows{rows: rows}, err
}

func (t Transaction) QueryRowContext(ctx context.Context, query *model.Query) model.Row {
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

type SavePoint struct {
	name string
	tx   Transaction
}

func (t Transaction) SavePoint() (model.SavePoint, error) {
	t.saves++
	point := "sp_" + strconv.FormatInt(t.saves, 10)
	_, err := t.tx.Exec(context.TODO(), "SAVEPOINT "+point)
	if err != nil {
		// goe can't log
		return nil, t.config.ErrorHandler(context.TODO(), err)
	}
	return SavePoint{point, t}, nil
}

func (s SavePoint) Rollback() error {
	_, err := s.tx.tx.Exec(context.TODO(), "ROLLBACK TO SAVEPOINT "+s.name)
	if err != nil {
		// goe can't log
		return s.tx.config.ErrorHandler(context.TODO(), err)
	}
	return nil
}

func (s SavePoint) Commit() error {
	_, err := s.tx.tx.Exec(context.TODO(), "RELEASE SAVEPOINT "+s.name)
	if err != nil {
		// goe can't log
		return s.tx.config.ErrorHandler(context.TODO(), err)
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
