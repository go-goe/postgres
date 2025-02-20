package postgres

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/olauro/goe"
)

type Driver struct {
	dns       string
	sql       *sql.DB
	selectt   []byte
	from      []byte
	where     []byte
	insert    []byte
	values    []byte
	returning []byte
	update    []byte
	set       []byte
	delete    []byte
}

func Open(dns string) (driver *Driver) {
	return &Driver{
		dns:       dns,
		selectt:   []byte("SELECT"),
		from:      []byte("FROM"),
		where:     []byte("WHERE"),
		insert:    []byte("INSERT INTO"),
		values:    []byte("VALUES"),
		returning: []byte("RETURNING"),
		update:    []byte("UPDATE"),
		set:       []byte("SET"),
		delete:    []byte("DELETE FROM"),
	}
}

func (dr *Driver) Init() {
	config, err := pgx.ParseConfig(dr.dns)
	if err != nil {
		//TODO: Add error handling
		fmt.Println(err)
		return
	}
	dr.sql = stdlib.OpenDB(*config)
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

func (dr *Driver) Select() []byte {
	return dr.selectt
}

func (dr *Driver) From() []byte {
	return dr.from
}

func (dr *Driver) Where() []byte {
	return dr.where
}

func (dr *Driver) Insert() []byte {
	return dr.insert
}

func (dr *Driver) Values() []byte {
	return dr.values
}

func (dr *Driver) Returning(b []byte) []byte {
	return append(dr.returning, b...)
}

func (dr *Driver) Update() []byte {
	return dr.update
}

func (dr *Driver) Set() []byte {
	return dr.set
}

func (dr *Driver) Delete() []byte {
	return dr.delete
}

func (dr *Driver) Stats() sql.DBStats {
	return dr.sql.Stats()
}

func (dr *Driver) NewConnection() goe.Connection {
	return Connection{sql: dr.sql}
}

type Connection struct {
	sql *sql.DB
}

func (c Connection) QueryContext(ctx context.Context, query string, args ...any) (goe.Rows, error) {
	rows, err := c.sql.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return Rows{rows: rows}, nil
}

func (c Connection) QueryRowContext(ctx context.Context, query string, args ...any) goe.Row {
	row := c.sql.QueryRowContext(ctx, query, args...)

	return Row{row: row}
}

func (c Connection) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := c.sql.ExecContext(ctx, query, args...)

	return err
}

func (dr *Driver) NewTransaction(ctx context.Context, opts *sql.TxOptions) (goe.Transaction, error) {
	tx, err := dr.sql.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return Transaction{tx: tx}, nil
}

type Transaction struct {
	tx *sql.Tx
}

func (t Transaction) QueryContext(ctx context.Context, query string, args ...any) (goe.Rows, error) {
	rows, err := t.tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return Rows{rows: rows}, nil
}

func (t Transaction) QueryRowContext(ctx context.Context, query string, args ...any) goe.Row {
	row := t.tx.QueryRowContext(ctx, query, args...)

	return Row{row: row}
}

func (t Transaction) ExecContext(ctx context.Context, query string, args ...any) error {
	_, err := t.tx.ExecContext(ctx, query, args...)

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
