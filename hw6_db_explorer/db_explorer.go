package main

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type dataType int

const (
	intType = iota
	stringType
)

type colDesc struct {
	dataType
	null    bool
	primary bool
}

type columns map[string]colDesc

type record map[string]interface{}

func (rec record) toFieldsValues() ([]string, []interface{}) {
	fields := make([]string, 0, len(rec))
	values := make([]interface{}, 0, len(rec))
	for field, value := range rec {
		fields = append(fields, field)
		values = append(values, value)
	}
	return fields, values
}

type table struct {
	name    string
	idField string
	columns
}

type DbExplorer struct {
	tables []table
	*sql.DB
	*http.ServeMux
}

func NewDbExplorer(db *sql.DB) (*DbExplorer, error) {
	err := db.Ping()
	if err != nil {
		return nil, fmt.Errorf("could not connect to db: %v", err)
	}

	explorer := &DbExplorer{DB: db}

	err = explorer.initTables()
	if err != nil {
		return nil, fmt.Errorf("could not init tables: %v", err)
	}

	explorer.initMux()

	return explorer, nil
}

func (e *DbExplorer) initTables() error {
	rows, err := e.Query("SHOW TABLES")
	if err != nil {
		return err
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		err := rows.Scan(&name)
		if err != nil {
			return err
		}
		names = append(names, name)
	}

	var tables []table
	for _, name := range names {
		t, err := e.getTable(name)
		if err != nil {
			return fmt.Errorf("could not get columns for table %s: %v", name, err)
		}
		tables = append(tables, *t)
	}

	e.tables = tables
	return nil
}

func (e *DbExplorer) getTable(name string) (*table, error) {
	rows, err := e.Query(" SHOW COLUMNS FROM " + name)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	t := table{
		name: name,
	}
	cols := make(columns)
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
			return nil, err
		}
		var dt dataType
		switch strings.Split(typeName, "(")[0] {
		case "int", "decimal":
			dt = intType
		default:
			dt = stringType
		}
		desc := colDesc{
			dataType: dt,
			null:     null == "YES",
			primary:  key == "PRI",
		}
		if desc.primary {
			t.idField = field
		}

		cols[field] = desc
	}
	t.columns = cols
	return &t, nil
}

func (e *DbExplorer) initMux() {
	mux := http.NewServeMux()
	mux.HandleFunc("/", e.rootHandler)
	for _, t := range e.tables {
		mux.HandleFunc("/"+t.name+"/", e.tableHandler(t))
	}

	e.ServeMux = mux
}

func (e *DbExplorer) response(w http.ResponseWriter, data interface{}) {
	response := struct {
		Response interface{} `json:"response"`
	}{Response: data}
	responseJson, err := json.Marshal(response)
	if err != nil {
		e.error(w, err, http.StatusInternalServerError)
	}
	w.Write(responseJson)
}

func (e *DbExplorer) error(w http.ResponseWriter, err error, code int) {
	message, errJson := json.Marshal(struct {
		Err string `json:"error"`
	}{Err: err.Error()})
	if errJson != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
	}
	http.Error(w, string(message), code)
}

func (e *DbExplorer) tableHandler(t table) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		path := strings.Split(r.URL.Path, "/")
		var id *int
		if len(path) > 2 {
			parsedId, err := strconv.Atoi(path[2])
			if err == nil {
				id = &parsedId
			}
		}
		switch r.Method {
		case "GET":
			if id == nil {
				e.selectAllHandler(t)(w, r)
			} else {
				e.selectByIdHandler(t, *id)(w, r)
			}
		case "PUT":
			e.insertHandler(t)(w, r)
		case "POST":
			if id == nil {
				e.error(w, errors.New("id not found"), http.StatusBadRequest)
				return
			}
			e.updateHandler(t, *id)(w, r)
		case "DELETE":
			if id == nil {
				e.error(w, errors.New("id not found"), http.StatusBadRequest)
				return
			}
			e.deleteHandler(t, *id)(w, r)
		}

	}
}

func (e *DbExplorer) selectAllHandler(t table) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defaultLimit := 5
		defaultOffset := 0


		var limit, offset int
		var err error
		limitStr := r.URL.Query().Get("limit")
		if limitStr == "" {
			limit = defaultLimit
		} else {
			limit, err = strconv.Atoi(limitStr)
			if err != nil {
				limit = defaultLimit
			}
		}

		offsetStr := r.URL.Query().Get("offset")
		if offsetStr == "" {
			offset = defaultOffset
		} else {
			offset, err = strconv.Atoi(offsetStr)
			if err != nil {
				offset = defaultOffset
			}
		}

		records, err := e.selectAll(t, limit, offset)
		if err != nil {
			e.error(w, err, http.StatusInternalServerError)
			return
		}
		e.response(w, struct {
			Records []record `json:"records"`
		}{Records: records})
	}
}

func (e *DbExplorer) selectAll(t table, limit, offset int) ([]record, error) {
	rows, err := e.Query("SELECT * FROM "+t.name+" LIMIT ? OFFSET ? ", limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	cols, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}

	values := makeValues(cols, t)
	var results []record
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

func (e *DbExplorer) selectByIdHandler(t table, id int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		result, ok, err := e.selectById(t, id)
		if err != nil {
			e.error(w, err, http.StatusInternalServerError)
			return
		}

		if !ok {
			e.error(w, errors.New("record not found"), http.StatusNotFound)
			return
		}

		e.response(w, struct {
			Record record `json:"record"`
		}{Record: result})
	}
}

func (e *DbExplorer) selectById(t table, id int) (record, bool, error) {
	rows, err := e.Query("SELECT * FROM "+t.name+" WHERE " + t.idField + " = ? ", id)
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

	values := makeValues(cols, t)
	err = rows.Scan(values...)
	if err != nil {
		return nil, false, err
	}

	result := convertResults(cols, values)
	return result, true, nil
}

func makeValues(colTypes []*sql.ColumnType, t table) []interface{} {
	values := make([]interface{}, len(colTypes))
	for i := range values {
		switch t.columns[colTypes[i].Name()].dataType {
		case intType:
			values[i] = new(sql.NullInt64)
		case stringType:
			values[i] = new(sql.NullString)
		default:
			values[i] = new(interface{})
		}
	}
	return values
}

func convertResults(cols []*sql.ColumnType, values []interface{}) record {
	result := make(record)
	for i, col := range cols {
		var value interface{} = nil
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

func (e *DbExplorer) rootHandler(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		e.error(w, errors.New("unknown table"), http.StatusNotFound)
		return
	}

	tableNames := make([]string, 0, len(e.tables))
	for _, t := range e.tables {
		tableNames = append(tableNames, t.name)
	}

	e.response(w, struct {
		Tables []string `json:"tables"`
	}{Tables: tableNames})
}

func (e *DbExplorer) insertHandler(t table) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		item, err := parseBody(r)
		if err != nil {
			e.error(w, err, http.StatusInternalServerError)
			return
		}

		delete(item, t.idField)

		err = validateParams(item, t)
		if err != nil {
			e.error(w, err, http.StatusBadRequest)
			return
		}

		for col, desc := range t.columns {
			if _, ok := item[col]; !ok && !desc.null && !desc.primary {
				switch desc.dataType {
				case intType:
					item[col] = 0
				case stringType:
					item[col] = ""
				}
			}
		}

		id, err := e.insertInto(t, item)
		if err != nil {
			e.error(w, err, http.StatusInternalServerError)
			return
		}

		resp := make(map[string]int64)
		resp[t.idField] = id
		e.response(w, resp)
	}
}

func (e *DbExplorer) insertInto(t table, item record) (int64, error) {
	fields, values := item.toFieldsValues()

	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("INSERT INTO ")
	queryBuilder.WriteString(t.name)
	queryBuilder.WriteString("(")
	queryBuilder.WriteString(strings.Join(fields, ", "))
	queryBuilder.WriteString(") VALUES(")
	queryBuilder.WriteString(strings.Repeat(",?", len(fields))[1:])
	queryBuilder.WriteString(")")

	result, err := e.Exec(queryBuilder.String(), values...)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func validateParams(r record, t table) error {
	for field, value := range r {
		desc, ok := t.columns[field]
		if !ok {
			delete(r, field)
			continue
		}

		if desc.null && value == nil {
			continue
		}

		switch desc.dataType {
		case stringType:
			_, ok = value.(string)
		case intType:
			_, ok = value.(float64)
		}

		if !ok {
			return fmt.Errorf("field %s have invalid type", field)
		}
	}
	return nil
}

func (e *DbExplorer) updateHandler(t table, id int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		item, err := parseBody(r)
		if err != nil {
			e.error(w, err, http.StatusInternalServerError)
			return
		}

		if _, ok := item[t.idField]; ok {
			e.error(w, fmt.Errorf("field %s have invalid type", t.idField), http.StatusBadRequest)
			return
		}

		err = validateParams(item, t)
		if err != nil {
			e.error(w, err, http.StatusBadRequest)
			return
		}

		updated, err := e.update(t, item, id)
		if err != nil {
			e.error(w, err, http.StatusInternalServerError)
			return
		}

		e.response(w, struct {
			Updated int64 `json:"updated"`
		}{Updated: updated})
	}
}

func (e *DbExplorer) update(t table, item record, id int) (int64, error) {
	fields, values := item.toFieldsValues()

	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("UPDATE ")
	queryBuilder.WriteString(t.name)
	queryBuilder.WriteString(" SET ")

	for i, field := range fields {
		if i > 0 {
			queryBuilder.WriteString(", ")
		}
		queryBuilder.WriteString(field)
		queryBuilder.WriteString(" = ?")
	}

	queryBuilder.WriteString(" WHERE ")
	queryBuilder.WriteString(t.idField)
	queryBuilder.WriteString(" = ? ")

	result, err := e.Exec(queryBuilder.String(), append(values, id)...)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

func parseBody(r *http.Request) (record, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	item := new(record)
	err = json.Unmarshal(body, item)
	if err != nil {
		return nil, err
	}

	return *item, nil
}

func (e *DbExplorer) deleteHandler(t table, id int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		deleted, err := e.delete(t, id)
		if err != nil {
			e.error(w, err, http.StatusInternalServerError)
			return
		}

		e.response(w, struct {
			Deleted int64 `json:"deleted"`
		}{Deleted: deleted})
	}
}

func (e *DbExplorer) delete(t table, id int) (int64, error) {
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("DELETE FROM ")
	queryBuilder.WriteString(t.name)
	queryBuilder.WriteString(" WHERE ")
	queryBuilder.WriteString(t.idField)
	queryBuilder.WriteString(" = ? ")

	result, err := e.Exec(queryBuilder.String(), id)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
