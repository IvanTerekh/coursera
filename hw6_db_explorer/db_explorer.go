package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type colDesc struct {
	dataType dataType
	null     bool
	primary  bool
}

type dataType int

const (
	intType = iota
	stringType
)

type table struct {
	name       string
	primaryKey string
	columns    map[string]colDesc
}

type record map[string]interface{}

type dataAccessObject struct {
	db     *sql.DB
	tables []table
}

type handlerError struct {
	httpCode int
	Msg      string `json:"error"`
}

type daoHandler func(dao *dataAccessObject, r *http.Request) (interface{}, *handlerError)

func NewDbExplorer(db *sql.DB) (http.Handler, error) {
	dao, err := newDataAccessObject(db)
	if err != nil {
		return nil, fmt.Errorf("could not init data access object: %v", err)
	}

	return newHandler(dao), nil
}

func newDataAccessObject(db *sql.DB) (*dataAccessObject, error) {
	tables, err := getTables(db)
	if err != nil {
		return nil, fmt.Errorf("could not get tables info: %v", err)
	}

	return &dataAccessObject{
		tables: tables,
		db:     db,
	}, nil
}

func (dao *dataAccessObject) tableNames() []string {
	names := make([]string, 0, len(dao.tables))
	for _, table := range dao.tables {
		names = append(names, table.name)
	}
	return names
}

func getTables(db *sql.DB) ([]table, error) {
	rows, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, fmt.Errorf("could not query table names: %v", err)
	}
	defer rows.Close()

	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, fmt.Errorf("could not scan table name: %v", err)
		}
		names = append(names, name)
	}

	var tables []table
	for _, name := range names {
		t, err := getTable(db, name)
		if err != nil {
			return nil, fmt.Errorf("could not get columns for table %s: %v", name, err)
		}
		tables = append(tables, *t)
	}

	return tables, nil
}

func getTable(db *sql.DB, name string) (*table, error) {
	rows, err := db.Query(" SHOW COLUMNS FROM " + name)
	if err != nil {
		return nil, fmt.Errorf("could not query columns info for table %v: %v", name, err)
	}
	defer rows.Close()

	t := table{
		name: name,
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
			return nil, fmt.Errorf("could not scan columns info for table %v: %v", name, err)
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
			t.primaryKey = field
		}

		cols[field] = desc
	}
	t.columns = cols
	return &t, nil
}

func newHandler(dao *dataAccessObject) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", responseHandler(dao, rootHandler))

	for _, table := range dao.tables {
		mux.HandleFunc("/"+table.name+"/", responseHandler(dao, tableHandler(table)))
	}

	return mux
}

func responseHandler(dao *dataAccessObject, handler daoHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response, handlerError := handler(dao, r)
		if handlerError != nil {
			errJSON, err := json.Marshal(handlerError)
			if err != nil {
				http.Error(w, `{"error": "internal server error"}`, http.StatusInternalServerError)
				return
			}
			http.Error(w, string(errJSON), handlerError.httpCode)
			return
		}

		responseJSON, err := json.Marshal(struct {
			Response interface{} `json:"response"`
		}{Response: response})
		if err != nil {
			http.Error(w, `{"error": "internal server error"}`, http.StatusInternalServerError)
			return
		}
		w.Write(responseJSON)
	}
}

func rootHandler(dao *dataAccessObject, r *http.Request) (interface{}, *handlerError) {
	if r.URL.Path != "/" {
		return nil, &handlerError{
			httpCode: http.StatusNotFound,
			Msg:      "unknown table",
		}
	}

	return struct {
		Tables []string `json:"tables"`
	}{Tables: dao.tableNames()}, nil
}

func tableHandler(table table) daoHandler {
	return func(dao *dataAccessObject, r *http.Request) (i interface{}, h *handlerError) {
		id, gotID := parseID(r.URL.Path)

		switch r.Method {
		case "GET":
			if gotID {
				return selectByIDHandler(table, id)(dao, r)
			}
			return selectAllHandler(table)(dao, r)
		case "PUT":
			return insertHandler(table)(dao, r)
		case "POST":
			if !gotID {
				return nil, &handlerError{
					httpCode: http.StatusBadRequest,
					Msg:      "id not found",
				}
			}
			return updateHandler(table, id)(dao, r)
		case "DELETE":
			if !gotID {
				return nil, &handlerError{
					httpCode: http.StatusBadRequest,
					Msg:      "id not found",
				}
			}
			return deleteHandler(table, id)(dao, r)
		}

		return nil, &handlerError{
			httpCode: http.StatusBadRequest,
			Msg:      "unsupported method",
		}
	}
}

func parseID(url string) (int, bool) {
	path := strings.Split(url, "/")
	if len(path) > 2 {
		id, err := strconv.Atoi(path[2])
		if err == nil {
			return id, true
		}
	}
	return -1, false
}

func selectByIDHandler(t table, id int) daoHandler {
	return func(dao *dataAccessObject, r *http.Request) (interface{}, *handlerError) {
		result, ok, err := dao.selectByID(t, id)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not select from %v by id: %v", t, err),
			}
		}

		if !ok {
			return nil, &handlerError{
				httpCode: http.StatusNotFound,
				Msg:      "record not found",
			}
		}

		return struct {
			Record record `json:"record"`
		}{Record: result}, nil
	}
}

func (dao *dataAccessObject) selectByID(table table, id int) (record, bool, error) {
	rows, err := dao.db.Query("SELECT * FROM "+table.name+" WHERE "+table.primaryKey+" = ? ", id)
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

func selectAllHandler(t table) daoHandler {
	return func(dao *dataAccessObject, r *http.Request) (i interface{}, i2 *handlerError) {
		limit := getIntParam(r, "limit", 5)
		offset := getIntParam(r, "offset", 0)

		records, err := dao.selectAll(t, limit, offset)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not select all from %v: %v", t, err),
			}
		}
		return struct {
			Records []record `json:"records"`
		}{Records: records}, nil
	}
}

func getIntParam(r *http.Request, key string, defaultVal int) int {
	paramStr := r.URL.Query().Get(key)
	if paramStr == "" {
		return defaultVal
	}

	param, err := strconv.Atoi(paramStr)
	if err != nil {
		return defaultVal
	}
	return param
}

func (dao *dataAccessObject) selectAll(t table, limit, offset int) ([]record, error) {
	rows, err := dao.db.Query("SELECT * FROM "+t.name+" LIMIT ? OFFSET ? ", limit, offset)
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

func insertHandler(t table) daoHandler {
	return func(dao *dataAccessObject, r *http.Request) (interface{}, *handlerError) {

		item, err := parseBody(r)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not parse input data: %v", err),
			}
		}

		delete(item, t.primaryKey)

		err = validateParams(item, t)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusBadRequest,
				Msg:      err.Error(),
			}
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

		id, err := dao.insertInto(t, item)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not insert item %v: %v", item, err),
			}
		}

		resp := make(map[string]int64)
		resp[t.primaryKey] = id
		return resp, nil
	}
}

func (dao *dataAccessObject) insertInto(t table, item record) (int64, error) {
	fields, values := item.toFieldsValues()

	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("INSERT INTO ")
	queryBuilder.WriteString(t.name)
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

func (rec record) toFieldsValues() ([]string, []interface{}) {
	fields := make([]string, 0, len(rec))
	values := make([]interface{}, 0, len(rec))
	for field, value := range rec {
		fields = append(fields, field)
		values = append(values, value)
	}
	return fields, values
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

func updateHandler(t table, id int) daoHandler {
	return func(dao *dataAccessObject, r *http.Request) (interface{}, *handlerError) {
		item, err := parseBody(r)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not parse input data: %v", err),
			}
		}

		if _, ok := item[t.primaryKey]; ok {
			return nil, &handlerError{
				httpCode: http.StatusBadRequest,
				Msg:      fmt.Sprintf("field %s have invalid type", t.primaryKey),
			}
		}

		err = validateParams(item, t)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusBadRequest,
				Msg:      err.Error(),
			}
		}

		updated, err := dao.update(t, item, id)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not update item %v: %v", item, err),
			}
		}

		return struct {
			Updated int64 `json:"updated"`
		}{Updated: updated}, nil
	}
}

func (dao *dataAccessObject) update(t table, item record, id int) (int64, error) {
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
	queryBuilder.WriteString(t.primaryKey)
	queryBuilder.WriteString(" = ? ")

	result, err := dao.db.Exec(queryBuilder.String(), append(values, id)...)
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

func deleteHandler(t table, id int) daoHandler {
	return func(dao *dataAccessObject, r *http.Request) (interface{}, *handlerError) {
		deleted, err := dao.delete(t, id)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not delete by id %v: %v", id, err),
			}
		}

		return struct {
			Deleted int64 `json:"deleted"`
		}{Deleted: deleted}, nil
	}
}

func (dao *dataAccessObject) delete(t table, id int) (int64, error) {
	queryBuilder := strings.Builder{}
	queryBuilder.WriteString("DELETE FROM ")
	queryBuilder.WriteString(t.name)
	queryBuilder.WriteString(" WHERE ")
	queryBuilder.WriteString(t.primaryKey)
	queryBuilder.WriteString(" = ? ")

	result, err := dao.db.Exec(queryBuilder.String(), id)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
