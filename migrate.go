package postgres

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/go-goe/goe"
	"github.com/go-goe/goe/enum"
	"github.com/go-goe/goe/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

func (db *Driver) MigrateContext(ctx context.Context, migrator *goe.Migrator) error {
	dataMap := map[string]string{
		"string":    "text",
		"int16":     "smallint",
		"int32":     "integer",
		"int64":     "bigint",
		"float32":   "real",
		"float64":   "double precision",
		"[]uint8":   "bytea",
		"time.Time": "timestamp",
		"bool":      "boolean",
		"uuid.UUID": "uuid",
	}

	sql := new(strings.Builder)
	sqlColumns := new(strings.Builder)
	schemes := strings.Builder{}
	dbSchemes, err := getSchemes(db.sql)
	if err != nil {
		return err
	}
	for _, s := range migrator.Schemes {
		if !slices.Contains(dbSchemes, s[1:len(s)-1]) {
			schemes.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %v;\n", s))
		}
	}

	for _, t := range migrator.Tables {
		err = checkTableChanges(t, dataMap, sql, db.sql)
		if err != nil {
			return err
		}

		err = checkIndex(t.Indexes, t, sqlColumns, db.sql)
		if err != nil {
			return err
		}
	}

	for _, t := range migrator.Tables {
		if !t.Migrated {
			createTable(t, dataMap, sql, migrator.Tables)
		}
	}

	sql.WriteString(sqlColumns.String())

	if sql.Len() != 0 {
		schemes.WriteString(sql.String())
		return db.rawExecContext(ctx, schemes.String())
	}
	return nil
}

func getSchemes(conn *pgxpool.Pool) ([]string, error) {
	rows, err := conn.Query(context.Background(), `
		SELECT nspname
		FROM pg_namespace
		WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema';
	`)
	if err != nil {
		return nil, err
	}

	var s string
	schemes := make([]string, 0)
	for rows.Next() {
		err = rows.Scan(&s)
		if err != nil {
			return nil, err
		}
		schemes = append(schemes, s)
	}
	return schemes, nil
}

func (db *Driver) rawExecContext(ctx context.Context, rawSql string, args ...any) error {
	query := model.Query{Type: enum.RawQuery, RawSql: rawSql, Arguments: args}
	query.Header.Err = wrapperExec(ctx, db.NewConnection(), &query)
	if query.Header.Err != nil {
		return db.GetDatabaseConfig().ErrorQueryHandler(ctx, query)
	}
	db.GetDatabaseConfig().InfoHandler(ctx, query)
	return nil
}

func wrapperExec(ctx context.Context, conn goe.Connection, query *model.Query) error {
	queryStart := time.Now()
	defer func() { query.Header.QueryDuration = time.Since(queryStart) }()
	return conn.ExecContext(ctx, query)
}

func (db *Driver) DropTable(scheme, table string) error {
	if len(scheme) > 2 {
		table = scheme + "." + table
	}
	return db.rawExecContext(context.TODO(), fmt.Sprintf("DROP TABLE IF EXISTS %v;", table))
}

func (db *Driver) RenameColumn(scheme, table, oldColumn, newColumn string) error {
	if len(scheme) > 2 {
		table = scheme + "." + table
	}
	return db.rawExecContext(context.TODO(), renameColumn(table, oldColumn, newColumn))
}

func (db *Driver) DropColumn(scheme, table, column string) error {
	if len(scheme) > 2 {
		table = scheme + "." + table
	}
	return db.rawExecContext(context.TODO(), dropColumn(table, column))
}

func renameColumn(table, oldColumnName, newColumnName string) string {
	return fmt.Sprintf("ALTER TABLE %v RENAME COLUMN %v TO %v;\n", table, oldColumnName, newColumnName)
}

func dropColumn(table, columnName string) string {
	return fmt.Sprintf("ALTER TABLE %v DROP COLUMN %v;\n", table, columnName)
}

func createTableSql(create, pks string, attributes []string, sql *strings.Builder) {
	sql.WriteString(create)
	for _, a := range attributes {
		sql.WriteString(a)
	}
	sql.WriteString(pks)
	sql.WriteString(");\n")
}

type dbColumn struct {
	columnName   string
	dataType     string
	defaultValue *string
	nullable     bool
}

type dbTable struct {
	columns map[string]*dbColumn
}

func checkTableChanges(table *goe.TableMigrate, dataMap map[string]string, sql *strings.Builder, conn *pgxpool.Pool) error {
	sqlTableInfos := `SELECT
	column_name, CASE 
	WHEN data_type = 'character varying' 
	THEN CONCAT('varchar','(',character_maximum_length,')')
	when data_type = 'integer' then case WHEN column_default like 'nextval%' THEN 'serial' ELSE data_type end
	when data_type = 'bigint' then case WHEN column_default like 'nextval%' THEN 'bigserial' ELSE data_type end
	when data_type like 'timestamp%' then 'timestamp'
	when data_type like 'numeric' then CONCAT('decimal', '(',numeric_precision, ',', numeric_scale, ')')
	ELSE data_type END, 
	column_default,
	CASE
	WHEN is_nullable = 'YES'
	THEN True
	ELSE False END AS is_nullable
	FROM information_schema.columns WHERE table_name = $1;
	`

	rows, err := conn.Query(context.Background(), sqlTableInfos, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	dts := make(map[string]*dbColumn)
	dt := dbColumn{}
	for rows.Next() {
		err = rows.Scan(&dt.columnName, &dt.dataType, &dt.defaultValue, &dt.nullable)
		if err != nil {
			return err
		}

		dts[dt.columnName] = &dbColumn{
			columnName:   dt.columnName,
			dataType:     dt.dataType,
			defaultValue: dt.defaultValue,
			nullable:     dt.nullable,
		}
	}
	if len(dts) == 0 {
		return nil
	}
	dbTable := dbTable{columns: dts}
	table.Migrated = true
	checkFields(conn, dbTable, table, dataMap, sql)

	return nil
}

func primaryKeyIsForeignKey(table *goe.TableMigrate, attName string) bool {
	return slices.ContainsFunc(table.ManyToOnes, func(m goe.ManyToOneMigrate) bool {
		return m.Name == attName
	}) || slices.ContainsFunc(table.OneToOnes, func(o goe.OneToOneMigrate) bool {
		return o.Name == attName
	})
}

func foreignKeyIsPrimarykey(table *goe.TableMigrate, attName string) bool {
	return slices.ContainsFunc(table.PrimaryKeys, func(pk goe.PrimaryKeyMigrate) bool {
		return pk.Name == attName
	})
}

func createTable(tbl *goe.TableMigrate, dataMap map[string]string, sql *strings.Builder, tables map[string]*goe.TableMigrate) {
	t := table{}
	t.name = fmt.Sprintf("CREATE TABLE %v (", tbl.EscapingTableName())
	for _, att := range tbl.PrimaryKeys {
		if primaryKeyIsForeignKey(tbl, att.Name) {
			continue
		}
		att.DataType = checkDataType(att.DataType, dataMap)
		if att.AutoIncrement {
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL,", att.EscapingName, checkTypeAutoIncrement(att.DataType)))
		} else {
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL,", att.EscapingName, att.DataType))
		}
	}

	for _, att := range tbl.Attributes {
		att.DataType = checkDataType(att.DataType, dataMap)
		t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v %v %v,", att.EscapingName, att.DataType, func() string {
			if att.Nullable {
				return "NULL"
			} else {
				return "NOT NULL"
			}
		}(), setDefault(att.Default)))
	}

	for _, att := range tbl.OneToOnes {
		tb := tables[att.TargetTable]
		if tb.Migrated {
			t.createAttrs = append(t.createAttrs, foreingOneToOne(att, dataMap))
		} else {
			if tb != tbl {
				createTable(tb, dataMap, sql, tables)
			}
			t.createAttrs = append(t.createAttrs, foreingOneToOne(att, dataMap))
		}
	}

	for _, att := range tbl.ManyToOnes {
		tb := tables[att.TargetTable]
		if tb.Migrated {
			t.createAttrs = append(t.createAttrs, foreingManyToOne(att, dataMap))
		} else {
			if tb != tbl {
				createTable(tb, dataMap, sql, tables)
			}
			t.createAttrs = append(t.createAttrs, foreingManyToOne(att, dataMap))
		}
	}

	tbl.Migrated = true
	t.createPk = fmt.Sprintf("primary key (%v", tbl.PrimaryKeys[0].EscapingName)
	for _, pk := range tbl.PrimaryKeys[1:] {
		t.createPk += fmt.Sprintf(",%v", pk.EscapingName)
	}
	t.createPk += ")"
	createTableSql(t.name, t.createPk, t.createAttrs, sql)
}

func setDefault(d string) string {
	if d == "" {
		return ""
	}

	return fmt.Sprintf("DEFAULT %v", d)
}

func foreingManyToOne(att goe.ManyToOneMigrate, dataMap map[string]string) string {
	att.DataType = checkDataType(att.DataType, dataMap)
	return fmt.Sprintf("%v %v %v REFERENCES %v(%v),", att.EscapingName, att.DataType, func() string {
		if att.Nullable {
			return "NULL"
		}
		return "NOT NULL"
	}(), att.EscapingTargetTableName(), att.EscapingTargetColumn)
}

func foreingOneToOne(att goe.OneToOneMigrate, dataMap map[string]string) string {
	att.DataType = checkDataType(att.DataType, dataMap)
	return fmt.Sprintf("%v %v UNIQUE %v REFERENCES %v(%v),",
		att.EscapingName,
		att.DataType,
		func() string {
			if att.Nullable {
				return "NULL"
			}
			return "NOT NULL"
		}(), att.EscapingTargetTableName(), att.EscapingTargetColumn)
}

type table struct {
	name        string
	createPk    string
	createAttrs []string
}

type databaseIndex struct {
	indexName string
	unique    bool
	attname   string
	table     string
	migrated  bool
}

func checkIndex(indexes []goe.IndexMigrate, table *goe.TableMigrate, sql *strings.Builder, conn *pgxpool.Pool) error {
	sqlQuery := `SELECT DISTINCT ci.relname, i.indisunique as is_unique, c.relname, a.attname FROM pg_index i
	JOIN pg_attribute a ON i.indexrelid = a.attrelid
	JOIN pg_class ci ON ci.oid = i.indexrelid
	JOIN pg_class c ON c.oid = i.indrelid
	where i.indisprimary = false AND c.relname = $1;
	`

	rows, err := conn.Query(context.Background(), sqlQuery, table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	dis := make(map[string]*databaseIndex)
	di := databaseIndex{}
	for rows.Next() {
		err = rows.Scan(&di.indexName, &di.unique, &di.table, &di.attname)
		if err != nil {
			return err
		}
		dis[di.indexName] = &databaseIndex{
			indexName: di.indexName,
			unique:    di.unique,
			attname:   di.attname,
			table:     di.table,
		}
	}

	for i := range indexes {
		if dbIndex, exist := dis[indexes[i].Name]; exist {
			if indexes[i].Unique != dbIndex.unique {
				sql.WriteString(fmt.Sprintf("DROP INDEX IF EXISTS %v;", indexes[i].EscapingName) + "\n")
				sql.WriteString(createIndex(indexes[i], table))
			}
			dbIndex.migrated = true
			continue
		}
		sql.WriteString(createIndex(indexes[i], table))
	}

	for _, dbIndex := range dis {
		if !dbIndex.migrated {
			if !slices.ContainsFunc(table.OneToOnes, func(o goe.OneToOneMigrate) bool {
				return o.Name == dbIndex.attname
			}) {
				sql.WriteString(fmt.Sprintf("DROP INDEX IF EXISTS %v;", keywordHandler(dbIndex.indexName)) + "\n")
			}
		}
	}
	return nil
}

func createIndex(index goe.IndexMigrate, table *goe.TableMigrate) string {
	return fmt.Sprintf("CREATE %v %v ON %v (%v);\n",
		func() string {
			if index.Unique {
				return "UNIQUE INDEX"
			}
			return "INDEX"
		}(),
		index.EscapingName,
		table.EscapingTableName(),
		func() string {
			s := fmt.Sprintf("%v", index.Attributes[0].EscapingName)
			for _, a := range index.Attributes[1:] {
				s += fmt.Sprintf(",%v", a.EscapingName)
			}
			return s
		}(),
	)
}

func checkFields(conn *pgxpool.Pool, dbTable dbTable, table *goe.TableMigrate, dataMap map[string]string, sql *strings.Builder) {
	for _, att := range table.PrimaryKeys {
		if column := dbTable.columns[att.Name]; column != nil {
			if primaryKeyIsForeignKey(table, att.Name) {
				continue
			}

			dataType := checkDataType(att.DataType, dataMap)
			if att.AutoIncrement {
				dataType = checkTypeAutoIncrement(dataType)
			}
			if column.dataType != dataType {
				if att.AutoIncrement {
					sql.WriteString(alterColumn(table.EscapingName, att.EscapingName, fmt.Sprintf("%v USING %v::%v", checkTypeAutoIncrement(dataType), att.EscapingName, checkTypeAutoIncrement(dataType)), dataMap))
					sql.WriteString(fmt.Sprintf("CREATE SEQUENCE %v_%v_seq OWNED BY %v.%v;\n", table.Name, att.Name, table.Name, att.Name))
					sql.WriteString(alterColumnDefault(table.EscapingName, att.EscapingName, fmt.Sprintf("nextval('%v_%v_seq'::regclass)", table.Name, att.Name)))
				} else {
					sql.WriteString(alterColumn(table.EscapingName, att.EscapingName, dataType, dataMap))
				}
			}
		}
	}

	for _, att := range table.Attributes {
		if column, exist := dbTable.columns[att.Name]; exist {
			dataType := checkDataType(att.DataType, dataMap)
			if column.dataType != dataType {
				sql.WriteString(alterColumn(table.EscapingName, att.EscapingName, dataType, dataMap))
			}
			if column.nullable != att.Nullable {
				sql.WriteString(nullableColumn(table.EscapingName, att.EscapingName, att.Nullable))
			}
			if column.defaultValue != nil {
				if att.Default == "" {
					// drop default
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP DEFAULT;",
						table.EscapingTableName(),
						att.EscapingName,
					))
					continue
				}
				if *column.defaultValue != att.Default {
					// update default
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;",
						table.EscapingTableName(),
						att.EscapingName,
						att.Default,
					))
					continue
				}
			}
			if att.Default != "" && column.defaultValue == nil {
				// create default
				sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;",
					table.EscapingTableName(),
					att.EscapingName,
					att.Default,
				))
			}
			continue
		}
		sql.WriteString(addColumn(table, att.EscapingName, checkDataType(att.DataType, dataMap), att.Nullable))
	}

	for _, att := range table.OneToOnes {
		if column, exist := dbTable.columns[att.Name]; exist {
			// change from many to one to one to one
			if _, unique := checkFkUnique(conn, table.Name, att.Name); !unique {
				if foreignKeyIsPrimarykey(table, att.Name) {
					continue
				}
				c := fmt.Sprintf("%v_%v_key", table.Name, column.columnName)
				sql.WriteString(fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v UNIQUE (%v);\n",
					table.EscapingTableName(),
					keywordHandler(c),
					att.EscapingName))
			}
			if column.nullable != att.Nullable {
				sql.WriteString(nullableColumn(table.EscapingName, att.EscapingName, att.Nullable))
			}
			continue
		}
		sql.WriteString(addColumnUnique(table.EscapingName, att.EscapingName, checkDataType(att.DataType, dataMap), att.Nullable))
		sql.WriteString(addFkOneToOne(table, att))
	}

	for _, att := range table.ManyToOnes {
		if column, exist := dbTable.columns[att.Name]; exist {
			// change from one to one to many to one
			if c, unique := checkFkUnique(conn, table.Name, att.Name); unique {
				sql.WriteString(fmt.Sprintf("ALTER TABLE %v DROP CONSTRAINT %v;\n", table.EscapingTableName(), keywordHandler(c)))
			}
			if column.nullable != att.Nullable {
				sql.WriteString(nullableColumn(table.EscapingName, att.EscapingName, att.Nullable))
			}
			continue
		}
		sql.WriteString(addColumn(table, att.EscapingName, checkDataType(att.DataType, dataMap), att.Nullable))
		sql.WriteString(addFkManyToOne(table, att))
	}
}

func checkFkUnique(conn *pgxpool.Pool, table, attribute string) (string, bool) {
	sql := `SELECT ci.relname, i.indisunique as is_unique FROM pg_index i
	JOIN pg_attribute a ON i.indexrelid = a.attrelid
	JOIN pg_class ci ON ci.oid = i.indexrelid
	JOIN pg_class c ON c.oid = i.indrelid
	where i.indisprimary = false AND c.relname = $1 AND a.attname = $2;`

	var b bool
	var s string
	row := conn.QueryRow(context.Background(), sql, table, attribute)
	row.Scan(&s, &b)
	return s, b
}

func addColumn(table *goe.TableMigrate, column, dataType string, nullable bool) string {
	return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v %v;\n", table, column, dataType,
		func() string {
			if nullable {
				return "NULL"
			}
			return "NOT NULL"
		}())
}

func addColumnUnique(table, column, dataType string, nullable bool) string {
	return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v UNIQUE %v;\n", table, column, dataType,
		func() string {
			if nullable {
				return "NULL"
			}
			return "NOT NULL"
		}())
}

func addFkManyToOne(table *goe.TableMigrate, att goe.ManyToOneMigrate) string {
	c := keywordHandler(fmt.Sprintf("fk_%v_%v", table.Name, att.Name))
	return fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v FOREIGN KEY (%v) REFERENCES %v (%v);\n",
		table.EscapingName,
		c,
		att.EscapingName,
		att.EscapingTargetTableName(),
		att.EscapingTargetColumn)
}

func addFkOneToOne(table *goe.TableMigrate, att goe.OneToOneMigrate) string {
	c := keywordHandler(fmt.Sprintf("fk_%v_%v", table.Name, att.Name))
	return fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v FOREIGN KEY (%v) REFERENCES %v (%v);\n",
		table.EscapingName,
		c,
		att.EscapingName,
		att.EscapingTargetTableName(),
		att.EscapingTargetColumn)
}

func checkDataType(structDataType string, dataMap map[string]string) string {
	switch structDataType {
	case "int8", "uint8", "uint16":
		structDataType = "int16"
	case "int", "uint", "uint32":
		structDataType = "int32"
	case "uint64":
		structDataType = "int64"
	}
	if dataMap[structDataType] != "" {
		structDataType = dataMap[structDataType]
	}
	return structDataType
}

func checkTypeAutoIncrement(structDataType string) string {
	dataMap := map[string]string{
		"smallint":    "smallserial",
		"integer":     "serial",
		"bigint":      "bigserial",
		"smallserial": "smallint",
		"serial":      "integer",
		"bigserial":   "bigint",
	}
	return dataMap[structDataType]
}

func alterColumn(table, column, dataType string, dataMap map[string]string) string {
	if dataMap[dataType] == "" {
		return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v;\n", table, column, dataType)
	}
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v;\n", table, column, dataMap[dataType])
}

func alterColumnDefault(table, column, defa string) string {
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;\n", table, column, defa)
}

func nullableColumn(table, columnName string, nullable bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP NOT NULL;\n", table, columnName)
	}
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET NOT NULL;\n", table, columnName)
}
