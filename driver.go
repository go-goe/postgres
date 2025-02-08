package postgres

import (
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/olauro/goe"
)

type Driver struct {
	dns       string
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

func (dr *Driver) Init(db *goe.DB) {
	if db.SqlDB != nil {
		return
	}
	config, err := pgx.ParseConfig(dr.dns)
	if err != nil {
		//TODO: Add error handling
		fmt.Println(err)
		return
	}
	db.SqlDB = stdlib.OpenDB(*config)
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
