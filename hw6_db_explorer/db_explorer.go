package main

import (
	"coursera/hw6_db_explorer/db"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"strconv"
	"strings"
)

type handlerError struct {
	httpCode int
	Msg      string `json:"error"`
}

type daoHandler func(dao *db.DataAccessObject, r *http.Request) (interface{}, *handlerError)

func NewDbExplorer(dbHandle *sql.DB) (http.Handler, error) {
	dao, err := db.New(dbHandle)
	if err != nil {
		return nil, fmt.Errorf("could not init data access object: %v", err)
	}

	return newHandler(dao), nil
}

func newHandler(dao *db.DataAccessObject) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/", responseHandler(dao, rootHandler))

	for _, table := range dao.Tables {
		mux.HandleFunc("/"+table.Name+"/", responseHandler(dao, tableHandler(table)))
	}

	return mux
}

func responseHandler(dao *db.DataAccessObject, handler daoHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		response, handlerError := handler(dao, r)
		if handlerError != nil {
			errJSON, err := json.Marshal(handlerError)
			if err != nil {
				http.Error(w, `{"error": "internal server error"}`, http.StatusInternalServerError)
				return
			}
			//log.Println(handlerError)
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

func rootHandler(dao *db.DataAccessObject, r *http.Request) (interface{}, *handlerError) {
	if r.URL.Path != "/" {
		return nil, &handlerError{
			httpCode: http.StatusNotFound,
			Msg:      "unknown table",
		}
	}

	return struct {
		Tables []string `json:"tables"`
	}{Tables: dao.TableNames()}, nil
}

func tableHandler(table db.Table) daoHandler {
	return func(dao *db.DataAccessObject, r *http.Request) (i interface{}, h *handlerError) {
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

func selectByIDHandler(table db.Table, id int) daoHandler {
	return func(dao *db.DataAccessObject, r *http.Request) (interface{}, *handlerError) {
		result, ok, err := dao.SelectByID(table, id)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not select from %v by id: %v", table, err),
			}
		}

		if !ok {
			return nil, &handlerError{
				httpCode: http.StatusNotFound,
				Msg:      "record not found",
			}
		}

		return struct {
			Record db.Record `json:"record"`
		}{Record: result}, nil
	}
}

func selectAllHandler(table db.Table) daoHandler {
	return func(dao *db.DataAccessObject, r *http.Request) (i interface{}, i2 *handlerError) {
		limit := getIntParam(r, "limit", 5)
		offset := getIntParam(r, "offset", 0)

		records, err := dao.SelectAll(table, limit, offset)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not select all from %v: %v", table, err),
			}
		}
		return struct {
			Records []db.Record `json:"records"`
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

func insertHandler(table db.Table) daoHandler {
	return func(dao *db.DataAccessObject, r *http.Request) (interface{}, *handlerError) {

		item, err := parseBody(r)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not parse input data: %v", err),
			}
		}

		delete(item, table.PrimaryKey)

		err = validateParams(item, table)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusBadRequest,
				Msg:      err.Error(),
			}
		}

		for col, desc := range table.Columns {
			if _, ok := item[col]; !ok && !desc.Null && !desc.Primary {
				switch desc.DataType {
				case db.IntType:
					item[col] = 0
				case db.StringType:
					item[col] = ""
				}
			}
		}

		id, err := dao.InsertInto(table, item)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not insert item %v: %v", item, err),
			}
		}

		resp := make(map[string]int64)
		resp[table.PrimaryKey] = id
		return resp, nil
	}
}

func validateParams(r db.Record, table db.Table) error {
	for field, value := range r {
		desc, ok := table.Columns[field]
		if !ok {
			delete(r, field)
			continue
		}

		if desc.Null && value == nil {
			continue
		}

		switch desc.DataType {
		case db.StringType:
			_, ok = value.(string)
		case db.IntType:
			_, ok = value.(float64)
		}

		if !ok {
			return fmt.Errorf("field %s have invalid type", field)
		}
	}
	return nil
}

func updateHandler(table db.Table, id int) daoHandler {
	return func(dao *db.DataAccessObject, r *http.Request) (interface{}, *handlerError) {
		item, err := parseBody(r)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusInternalServerError,
				Msg:      fmt.Sprintf("could not parse input data: %v", err),
			}
		}

		if _, ok := item[table.PrimaryKey]; ok {
			return nil, &handlerError{
				httpCode: http.StatusBadRequest,
				Msg:      fmt.Sprintf("field %s have invalid type", table.PrimaryKey),
			}
		}

		err = validateParams(item, table)
		if err != nil {
			return nil, &handlerError{
				httpCode: http.StatusBadRequest,
				Msg:      err.Error(),
			}
		}

		updated, err := dao.Update(table, item, id)
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

func parseBody(r *http.Request) (db.Record, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	item := new(db.Record)
	err = json.Unmarshal(body, item)
	if err != nil {
		return nil, err
	}

	return *item, nil
}

func deleteHandler(table db.Table, id int) daoHandler {
	return func(dao *db.DataAccessObject, r *http.Request) (interface{}, *handlerError) {
		deleted, err := dao.Delete(table, id)
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
