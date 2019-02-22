package db

import (
	"database/sql"
	"fmt"
	"strings"
)

type colDesc struct {
	DataType DataType
	Null     bool
	Primary  bool
}

type DataType int

const (
	IntType = iota
	StringType
)

type Table struct {
	Name       string
	PrimaryKey string
	Columns    map[string]colDesc
}

type Record map[string]interface{}

func (rec Record) toFieldsValues() ([]string, []interface{}) {
	fields := make([]string, 0, len(rec))
	values := make([]interface{}, 0, len(rec))
	for field, value := range rec {
		fields = append(fields, field)
		values = append(values, value)
	}
	return fields, values
}

type DataAccessObject struct {
	db     *sql.DB
	Tables []Table
}

func New(db *sql.DB) (*DataAccessObject, error) {
	tables, err := getTables(db)
	if err != nil {
		return nil, fmt.Errorf("could not get tables info: %v", err)
	}

	return &DataAccessObject{
		Tables: tables,
		db:     db,
	}, nil
}

func getTables(db *sql.DB) ([]Table, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("could not query Table names: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("could not scan Table name: %v", err)
		}
		names = append(names, name)
	}

	var tables []Table
	for _, name := range names {
		t, err := getTable(db, name)
		if err != nil {
			return nil, fmt.Errorf("could not get columns for Table %s: %v", name, err)
		}
		tables = append(tables, *t)
	}

	return tables, nil
}

func getTable(db *sql.DB, name string) (*Table, error) {
	rows, err := db.Query("SHOW COLUMNS FROM " + name)
	if err != nil {
		return nil, fmt.Errorf("could not query columns info for Table %v: %v", name, err)
	}
	defer rows.Close()

	t := Table{
		Name: name,
	}
	cols := make(map[string]colDesc)
	for rows.Next() {
		var field, typeName, null, key string
		err = rows.Scan(
			&field,
			&typeName,
			&null,
			&key,
			new(interface{}),
			new(interface{}),
		)
		if err != nil {
			return nil, fmt.Errorf("could not scan columns info for Table %v: %v", name, err)
		}
		var dt DataType
		switch strings.Split(typeName, "(")[0] {
		case "int", "decimal":
			dt = IntType
		default:
			dt = StringType
		}
		desc := colDesc{
			DataType: dt,
			Null:     null == "YES",
			Primary:  key == "PRI",
		}
		if desc.Primary {
			t.PrimaryKey = field
		}

		cols[field] = desc
	}
	t.Columns = cols
	return &t, nil
}

func (dao *DataAccessObject) TableNames() []string {
	names := make([]string, 0, len(dao.Tables))
	for _, table := range dao.Tables {
		names = append(names, table.Name)
	}
	return names
}

func (dao *DataAccessObject) SelectByID(table Table, id int) (Record, bool, error) {
	rows, err := dao.db.Query("SELECT * FROM "+table.Name+" WHERE "+table.PrimaryKey+"= ? ", id)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, false, nil
	}

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, false, err
	}

	values := makeValues(cols, table)
	err = rows.Scan(values...)
	if err != nil {
		return nil, false, err
	}

	result := convertResults(cols, values)
	return result, true, nil
}

func (dao *DataAccessObject) SelectAll(t Table, limit, offset int) ([]Record, error) {
	rows, err := dao.db.Query("SELECT * FROM "+t.Name+" LIMIT ? OFFSET ? ", limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	values := makeValues(cols, t)
	var results []Record
	for rows.Next() {
		err = rows.Scan(values...)
		if err != nil {
			return nil, err
		}

		result := convertResults(cols, values)
		results = append(results, result)
	}

	return results, nil
}

func (dao *DataAccessObject) InsertInto(t Table, item Record) (int64, error) {
	fields, values := item.toFieldsValues()

	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("INSERT INTO ")
	queryBuilder.WriteString(t.Name)
	queryBuilder.WriteString("(")
	queryBuilder.WriteString(strings.Join(fields, ", "))
	queryBuilder.WriteString(") VALUES(")
	queryBuilder.WriteString(strings.Repeat(",?", len(fields))[1:])
	queryBuilder.WriteString(")")

	result, err := dao.db.Exec(queryBuilder.String(), values...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (dao *DataAccessObject) Update(t Table, item Record, id int) (int64, error) {
	fields, values := item.toFieldsValues()

	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("UPDATE ")
	queryBuilder.WriteString(t.Name)
	queryBuilder.WriteString(" SET ")

	for i, field := range fields {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(field)
		queryBuilder.WriteString("=?")
	}

	queryBuilder.WriteString(" WHERE ")
	queryBuilder.WriteString(t.PrimaryKey)
	queryBuilder.WriteString("=?")

	result, err := dao.db.Exec(queryBuilder.String(), append(values, id)...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func (dao *DataAccessObject) Delete(t Table, id int) (int64, error) {
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("DELETE FROM ")
	queryBuilder.WriteString(t.Name)
	queryBuilder.WriteString(" WHERE ")
	queryBuilder.WriteString(t.PrimaryKey)
	queryBuilder.WriteString("=?")

	result, err := dao.db.Exec(queryBuilder.String(), id)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func makeValues(colTypes []*sql.ColumnType, t Table) []interface{} {
	values := make([]interface{}, len(colTypes))
	for i := range values {
		switch t.Columns[colTypes[i].Name()].DataType {
		case IntType:
			values[i] = new(sql.NullInt64)
		case StringType:
			values[i] = new(sql.NullString)
		default:
			values[i] = new(interface{})
		}
	}
	return values
}

func convertResults(cols []*sql.ColumnType, values []interface{}) Record {
	result := make(Record)
	for i, col := range cols {
		var value interface{}
		switch values[i].(type) {
		case *sql.NullString:
			nullString := values[i].(*sql.NullString)
			if nullString.Valid {
				value = nullString.String
			}
		case *sql.NullInt64:
			nullInt := values[i].(*sql.NullInt64)
			if nullInt.Valid {
				value = nullInt.Int64
			}
		}
		result[col.Name()] = value
	}
	return result
}
