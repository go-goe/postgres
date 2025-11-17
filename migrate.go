package postgres

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/go-goe/goe/enum"
	"github.com/go-goe/goe/model"
	"github.com/jackc/pgx/v5/pgxpool"
)

type dataType struct {
	typeName  string
	zeroValue string
}

func (db *Driver) MigrateContext(ctx context.Context, migrator *model.Migrator) error {
	dataMap := map[string]dataType{
		"string":    {"text", "''"},
		"int16":     {"smallint", "0"},
		"int32":     {"integer", "0"},
		"int64":     {"bigint", "0"},
		"float32":   {"real", "0"},
		"float64":   {"double precision", "0"},
		"[]uint8":   {"bytea", "''"},
		"time.Time": {"timestamp", "to_timestamp(0)"},
		"bool":      {"boolean", "false"},
		"uuid.UUID": {"uuid", "'00000000-0000-0000-0000-000000000000'"},
	}

	sql := new(strings.Builder)
	sqlColumns := new(strings.Builder)
	schemas := strings.Builder{}
	dbSchemas, err := getSchemas(db.sql)
	if err != nil {
		return err
	}
	for _, s := range migrator.Schemas {
		if !slices.Contains(dbSchemas, s[1:len(s)-1]) {
			schemas.WriteString(fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %v;\n", s))
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
		schemas.WriteString(sql.String())
		return db.rawExecContext(ctx, schemas.String())
	}
	return nil
}

func getSchemas(conn *pgxpool.Pool) ([]string, error) {
	rows, err := conn.Query(context.Background(), `
		SELECT nspname
		FROM pg_namespace
		WHERE nspname NOT LIKE 'pg_%' AND nspname <> 'information_schema';
	`)
	if err != nil {
		return nil, err
	}

	var s string
	schemas := make([]string, 0)
	for rows.Next() {
		err = rows.Scan(&s)
		if err != nil {
			return nil, err
		}
		schemas = append(schemas, s)
	}
	return schemas, nil
}

func (db *Driver) rawExecContext(ctx context.Context, rawSql string, args ...any) error {
	if db.config.MigratePath == "" {
		query := model.Query{Type: enum.RawQuery, RawSql: rawSql, Arguments: args}
		query.Header.Err = wrapperExec(ctx, db.NewConnection(), &query)
		if query.Header.Err != nil {
			return db.GetDatabaseConfig().ErrorQueryHandler(ctx, query)
		}
		db.GetDatabaseConfig().InfoHandler(ctx, query)
		return nil
	}
	root, err := os.OpenRoot(db.config.MigratePath)
	if err != nil {
		return err
	}
	defer root.Close()

	file, err := root.OpenFile(db.Name()+"_"+strconv.FormatInt(time.Now().Unix(), 10)+".sql", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.WriteString(rawSql)
	return err
}

func wrapperExec(ctx context.Context, conn model.Connection, query *model.Query) error {
	queryStart := time.Now()
	defer func() { query.Header.QueryDuration = time.Since(queryStart) }()
	return conn.ExecContext(ctx, query)
}

func (db *Driver) DropTable(schema, table string) error {
	if len(schema) > 2 {
		table = schema + "." + table
	}
	return db.rawExecContext(context.TODO(), fmt.Sprintf("DROP TABLE IF EXISTS %v;", table))
}

func (db *Driver) RenameTable(schema, table, newTable string) error {
	if len(schema) > 2 {
		table = schema + "." + table
		newTable = schema + "." + newTable
	}
	return db.rawExecContext(context.TODO(), fmt.Sprintf("ALTER TABLE %v RENAME TO %v;", table, newTable))
}

func (db *Driver) RenameColumn(schema, table, oldColumn, newColumn string) error {
	if len(schema) > 2 {
		table = schema + "." + table
	}
	return db.rawExecContext(context.TODO(), renameColumn(table, oldColumn, newColumn))
}

func (db *Driver) DropColumn(schema, table, column string) error {
	if len(schema) > 2 {
		table = schema + "." + table
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

func checkTableChanges(table *model.TableMigrate, dataMap map[string]dataType, sql *strings.Builder, conn *pgxpool.Pool) error {
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

func primaryKeyIsForeignKey(table *model.TableMigrate, attName string) bool {
	return slices.ContainsFunc(table.ManyToOnes, func(m model.ManyToOneMigrate) bool {
		return m.Name == attName
	}) || slices.ContainsFunc(table.OneToOnes, func(o model.OneToOneMigrate) bool {
		return o.Name == attName
	})
}

func foreignKeyIsPrimarykey(table *model.TableMigrate, attName string) bool {
	return slices.ContainsFunc(table.PrimaryKeys, func(pk model.PrimaryKeyMigrate) bool {
		return pk.Name == attName
	})
}

func createTable(tbl *model.TableMigrate, dataMap map[string]dataType, sql *strings.Builder, tables map[string]*model.TableMigrate) {
	t := table{}
	t.name = fmt.Sprintf("CREATE TABLE %v (", tbl.EscapingTableName())
	for _, att := range tbl.PrimaryKeys {
		if primaryKeyIsForeignKey(tbl, att.Name) {
			continue
		}
		att.DataType = checkDataType(att.DataType, dataMap).typeName
		if att.AutoIncrement {
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL,", att.EscapingName, checkTypeAutoIncrement(att.DataType)))
		} else {
			t.createAttrs = append(t.createAttrs, fmt.Sprintf("%v %v NOT NULL %v,", att.EscapingName, att.DataType, setDefault(att.Default)))
		}
	}

	for _, att := range tbl.Attributes {
		att.DataType = checkDataType(att.DataType, dataMap).typeName
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

func foreingManyToOne(att model.ManyToOneMigrate, dataMap map[string]dataType) string {
	att.DataType = checkDataType(att.DataType, dataMap).typeName
	return fmt.Sprintf("%v %v %v REFERENCES %v(%v),", att.EscapingName, att.DataType, func() string {
		if att.Nullable {
			return "NULL"
		}
		return "NOT NULL"
	}(), att.EscapingTargetTableName(), att.EscapingTargetColumn)
}

func foreingOneToOne(att model.OneToOneMigrate, dataMap map[string]dataType) string {
	att.DataType = checkDataType(att.DataType, dataMap).typeName
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

func checkIndex(indexes []model.IndexMigrate, table *model.TableMigrate, sql *strings.Builder, conn *pgxpool.Pool) error {
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
			if !slices.ContainsFunc(table.OneToOnes, func(o model.OneToOneMigrate) bool {
				return o.Name == dbIndex.attname
			}) {
				sql.WriteString(fmt.Sprintf("DROP INDEX IF EXISTS %v;", keywordHandler(dbIndex.indexName)) + "\n")
			}
		}
	}
	return nil
}

func createIndex(index model.IndexMigrate, table *model.TableMigrate) string {
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

func checkFields(conn *pgxpool.Pool, dbTable dbTable, table *model.TableMigrate, dataMap map[string]dataType, sql *strings.Builder) {
	for _, att := range table.PrimaryKeys {
		if column := dbTable.columns[att.Name]; column != nil {
			if primaryKeyIsForeignKey(table, att.Name) {
				continue
			}

			dataType := checkDataType(att.DataType, dataMap).typeName
			if att.AutoIncrement {
				dataType = checkTypeAutoIncrement(dataType)
			}
			if column.dataType != dataType {
				if att.AutoIncrement {
					sql.WriteString(alterColumn(table, att.EscapingName, fmt.Sprintf("%v USING %v::%v", checkTypeAutoIncrement(dataType), att.EscapingName, checkTypeAutoIncrement(dataType)), dataMap))
					sql.WriteString(fmt.Sprintf("CREATE SEQUENCE %v_%v_seq OWNED BY %v.%v;\n", table.Name, att.Name, table.Name, att.Name))
					sql.WriteString(alterColumnDefault(table, att.EscapingName, fmt.Sprintf("nextval('%v_%v_seq'::regclass)", table.Name, att.Name)))
				} else {
					sql.WriteString(alterColumn(table, att.EscapingName, dataType, dataMap))
				}
			}
			if !att.AutoIncrement && column.defaultValue != nil {
				if att.Default == "" {
					// drop default
					sql.WriteString(fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP DEFAULT;",
						table.EscapingTableName(),
						att.EscapingName,
					))
					continue
				}
				if !strings.HasPrefix(*column.defaultValue, att.Default) {
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
		}
	}

	for _, att := range table.Attributes {
		if column, exist := dbTable.columns[att.Name]; exist {
			dataType := checkDataType(att.DataType, dataMap).typeName
			if column.dataType != dataType {
				sql.WriteString(alterColumn(table, att.EscapingName, dataType, dataMap))
			}
			if column.nullable != att.Nullable {
				sql.WriteString(nullableColumn(table, att.EscapingName, att.Nullable))
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
		dt := checkDataType(att.DataType, dataMap)
		if att.Default != "" {
			dt.zeroValue = att.Default
			sql.WriteString(addColumn(table, att.EscapingName, dt, att.Nullable, false))
			continue
		}
		sql.WriteString(addColumn(table, att.EscapingName, dt, att.Nullable, true))
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
				sql.WriteString(nullableColumn(table, att.EscapingName, att.Nullable))
			}
			continue
		}
		sql.WriteString(addColumnUnique(table, att.EscapingName, checkDataType(att.DataType, dataMap), att.Nullable))
		sql.WriteString(addFkOneToOne(table, att))
	}

	for _, att := range table.ManyToOnes {
		if column, exist := dbTable.columns[att.Name]; exist {
			// change from one to one to many to one
			if c, unique := checkFkUnique(conn, table.Name, att.Name); unique {
				sql.WriteString(fmt.Sprintf("ALTER TABLE %v DROP CONSTRAINT %v;\n", table.EscapingTableName(), keywordHandler(c)))
			}
			if column.nullable != att.Nullable {
				sql.WriteString(nullableColumn(table, att.EscapingName, att.Nullable))
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
		dt := checkDataType(att.DataType, dataMap)
		if att.Default != "" {
			dt.zeroValue = att.Default
			sql.WriteString(addColumn(table, att.EscapingName, dt, att.Nullable, false))
			sql.WriteString(addFkManyToOne(table, att))
			continue
		}
		sql.WriteString(addColumn(table, att.EscapingName, dt, att.Nullable, true))
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

func addColumn(table *model.TableMigrate, column string, dataType dataType, nullable bool, dropDefault bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v NULL;\n", table.EscapingTableName(), column, dataType.typeName)
	}
	if dropDefault {
		return fmt.Sprintf("ALTER TABLE %[1]v ADD COLUMN %[2]v %[3]v NOT NULL DEFAULT %[4]v;\n ALTER TABLE %[1]v ALTER COLUMN %[2]v DROP DEFAULT;\n",
			table.EscapingTableName(), column, dataType.typeName, dataType.zeroValue)
	}
	return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v NOT NULL DEFAULT %v;\n",
		table.EscapingTableName(), column, dataType.typeName, dataType.zeroValue)
}

func addColumnUnique(table *model.TableMigrate, column string, dataType dataType, nullable bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ADD COLUMN %v %v UNIQUE NULL;\n", table.EscapingTableName(), column, dataType)
	}
	return fmt.Sprintf("ALTER TABLE %[1]v ADD COLUMN %[2]v %[3]v UNIQUE NOT NULL DEFAULT %[4]v;\n ALTER TABLE %[1]v ALTER COLUMN %[2]v DROP DEFAULT;\n",
		table.EscapingTableName(), column, dataType.typeName, dataType.zeroValue)
}

func addFkManyToOne(table *model.TableMigrate, att model.ManyToOneMigrate) string {
	c := keywordHandler(fmt.Sprintf("fk_%v_%v", table.Name, att.Name))
	return fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v FOREIGN KEY (%v) REFERENCES %v (%v);\n",
		table.EscapingTableName(),
		c,
		att.EscapingName,
		att.EscapingTargetTableName(),
		att.EscapingTargetColumn)
}

func addFkOneToOne(table *model.TableMigrate, att model.OneToOneMigrate) string {
	c := keywordHandler(fmt.Sprintf("fk_%v_%v", table.Name, att.Name))
	return fmt.Sprintf("ALTER TABLE %v ADD CONSTRAINT %v FOREIGN KEY (%v) REFERENCES %v (%v);\n",
		table.EscapingTableName(),
		c,
		att.EscapingName,
		att.EscapingTargetTableName(),
		att.EscapingTargetColumn)
}

func checkDataType(structDataType string, dataMap map[string]dataType) dataType {
	dt := dataType{typeName: structDataType}
	switch structDataType {
	case "int8", "uint8", "uint16":
		dt = dataType{"int16", "0"}
	case "int", "uint", "uint32":
		dt = dataType{"int32", "0"}
	case "uint64":
		dt = dataType{"int64", "0"}
	}

	if dt, ok := dataMap[dt.typeName]; ok {
		return dt
	}

	for _, s := range []string{"number", "numeric", "decimal"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "0"}
		}
	}

	for _, s := range []string{"date", "time"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "0000-01-01"}
		}
	}

	for _, s := range []string{"char", "varchar", "text"} {
		if strings.Contains(strings.ToLower(structDataType), s) {
			return dataType{structDataType, "''"}
		}
	}

	return dt
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

func alterColumn(table *model.TableMigrate, column, dataType string, dataMap map[string]dataType) string {
	if dt, ok := dataMap[dataType]; ok {
		return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v;\n", table.EscapingTableName(), column, dt.typeName)
	}
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v TYPE %v;\n", table.EscapingTableName(), column, dataType)
}

func alterColumnDefault(table *model.TableMigrate, column, defa string) string {
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET DEFAULT %v;\n", table.EscapingTableName(), column, defa)
}

func nullableColumn(table *model.TableMigrate, columnName string, nullable bool) string {
	if nullable {
		return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v DROP NOT NULL;\n", table.EscapingTableName(), columnName)
	}
	return fmt.Sprintf("ALTER TABLE %v ALTER COLUMN %v SET NOT NULL;\n", table.EscapingTableName(), columnName)
}
