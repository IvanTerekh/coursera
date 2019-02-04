package main

import (
	"encoding/json"
	"encoding/xml"
	"log"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

var handler = SearchServer{dataStore: "dataset.xml"}
var server = httptest.NewServer(handler)
var searchClient = SearchClient{URL: server.URL}

func TestReadAll(t *testing.T) {
	limit := 25
	offset := 0
	nextPage := true

	for nextPage {
		res, err := searchClient.FindUsers(SearchRequest{
			Limit: limit,
			Offset: offset,
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		nextPage = res.NextPage
		offset += limit
	}
}

func TestLimits(t *testing.T) {
	limits := []int{1, 2, 5, 10}
	for _, limit := range limits {
		res, err := searchClient.FindUsers(SearchRequest{
			Limit: limit,
		})
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if len(res.Users) > limit {
			t.Error("Number of user exceeds limit")
		}
	}

	limit := -4
	_, err := searchClient.FindUsers(SearchRequest{
		Limit: limit,
	})
	if err == nil {
		t.Errorf("did not get an error for invalid limit %v", limit)
	}

	limit = 112
	maxLimit := 25
	res, err := searchClient.FindUsers(SearchRequest{
		Limit: limit,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if len(res.Users) > maxLimit {
		t.Error("Number of user exceeds limit")
	}
}

func TestBadRequest(t *testing.T) {
	limit := 10
	orderField := "phone"
	_, err := searchClient.FindUsers(SearchRequest{
		Limit:      limit,
		OrderField: orderField,
	})
	if err == nil {
		t.Error("did not get an error for invalid order field")
	}

	orderBy := 42
	_, err = searchClient.FindUsers(SearchRequest{
		Limit:   limit,
		OrderBy: orderBy,
	})
	if err == nil {
		t.Error("did not get an error for invalid orderBy parameter")
	}

	handler := badServer{
		handle: func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "error:)", http.StatusBadRequest)
		},
	}
	server := httptest.NewServer(handler)
	searchClient := SearchClient{URL: server.URL}
	_, err = searchClient.FindUsers(SearchRequest{
		Limit: limit,
	})
	if err == nil {
		t.Errorf("did non get an error for bad request")
	}
}

func TestOffset(t *testing.T) {
	offset := 0
	res, err := searchClient.FindUsers(SearchRequest{
		Offset: offset,
		Limit:  10,
	})
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if res.Users[0].Id != offset {
		t.Errorf("wrong result for offset: %v", offset)
	}

	offset = -4
	_, err = searchClient.FindUsers(SearchRequest{
		Offset: offset,
		Limit:  10,
	})
	if err == nil {
		t.Errorf("Did non get an error for invalid offset: %v", offset)
	}
}

func TestServerErrors(t *testing.T) {
	handler := badServer{
		handle: func(w http.ResponseWriter, r *http.Request) {
			time.Sleep(5 * time.Second)
		},
	}
	server := httptest.NewServer(handler)
	searchClient := SearchClient{URL: server.URL}
	_, err := searchClient.FindUsers(SearchRequest{
		Limit: 10,
	})
	if err == nil {
		t.Errorf("did non get an error for timout exceed")
	}

	handler.handle = func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, r.URL.Path, http.StatusMovedPermanently)
	}
	server = httptest.NewServer(handler)
	searchClient = SearchClient{URL: server.URL}
	_, err = searchClient.FindUsers(SearchRequest{
		Limit: 10,
	})
	if err == nil {
		t.Errorf("did non get an error for infinite redirect")
	}

	handler.handle =
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "error:)", http.StatusInternalServerError)
		}
	server = httptest.NewServer(handler)
	searchClient = SearchClient{URL: server.URL}
	_, err = searchClient.FindUsers(SearchRequest{
		Limit: 10,
	})
	if err == nil {
		t.Errorf("did non get an error for internal server error")
	}

	handler.handle =
		func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "error:)", http.StatusUnauthorized)
		}
	server = httptest.NewServer(handler)
	searchClient = SearchClient{URL: server.URL}
	_, err = searchClient.FindUsers(SearchRequest{
		Limit: 10,
	})
	if err == nil {
		t.Errorf("did non get an error for unauthorized request")
	}

	handler.handle =
		func(w http.ResponseWriter, r *http.Request) {
			_, err := w.Write([]byte("[{]"))
			if err != nil {
				panic(err)
			}
		}
	server = httptest.NewServer(handler)
	searchClient = SearchClient{URL: server.URL}
	_, err = searchClient.FindUsers(SearchRequest{
		Limit: 10,
	})
	if err == nil {
		t.Errorf("did non get an error for unauthorized request")
	}
}

type badServer struct {
	handle func(w http.ResponseWriter, r *http.Request)
}

func (server badServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	server.handle(w, r)
}

type SearchServer struct {
	dataStore string
}

func (server SearchServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	searchRequest, err := parseSearchRequest(r)
	if err != nil {
		http.Error(w, "could not parse search requst", http.StatusBadRequest)
		log.Println(err)
		return
	}

	file, err := os.Open(server.dataStore)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		log.Println(err)
		return
	}

	type container struct {
		Rows []struct {
			Id        int    `xml:"id"`
			Name      string `xml:"name"`
			Age       int    `xml:"age"`
			About     string `xml:"about"`
			Gender    string `xml:"gender"`
			FirstName string `xml:"first_name"`
			LastName  string `xml:"last_name"`
		} `xml:"row"`
	}

	var data container
	decoder := xml.NewDecoder(file)
	err = decoder.Decode(&data)
	if err != nil {
		http.Error(w, "internal server error", http.StatusInternalServerError)
		log.Println(err)
		return
	}

	var searchResults []User
	for _, row := range data.Rows {
		row.Name = row.FirstName + " " + row.LastName
		if strings.Contains(row.FirstName, searchRequest.Query) ||
			strings.Contains(row.LastName, searchRequest.Query) ||
			strings.Contains(row.About, searchRequest.Query) {

			if searchRequest.Offset > 0 {
				searchRequest.Offset--
				continue
			}

			newResult := User{
				Id:     row.Id,
				Name:   row.Name,
				Age:    row.Age,
				About:  row.About,
				Gender: row.Gender,
			}
			searchResults = append(searchResults, newResult)

			if len(searchResults) == searchRequest.Limit {
				break
			}
		}
	}

	var less func(i, j int) bool
	switch searchRequest.OrderField {
	case "Id":
		less = func(i, j int) bool {
			return searchResults[i].Id < searchResults[j].Id
		}
	case "Age":
		less = func(i, j int) bool {
			return searchResults[i].Age < searchResults[j].Age
		}
	case "Name", "":
		less = func(i, j int) bool {
			return searchResults[i].Name < searchResults[j].Name
		}
	default:
		http.Error(w, `{"error":"ErrorBadOrderField"}`, http.StatusBadRequest)
		return
	}

	switch searchRequest.OrderBy {
	case OrderByAsc:
		sort.Slice(searchResults, less)
	case OrderByDesc:
		sort.Slice(searchResults, func(i, j int) bool { return !less(i, j) })
	case OrderByAsIs:
	default:
		http.Error(w, `{"error":"ErrorBadOrderBy"}`, http.StatusBadRequest)
		return
	}

	jsonResult, err := json.Marshal(searchResults)
	_, err = w.Write(jsonResult)
	if err != nil {
		log.Println(err)
		return
	}
}

func parseSearchRequest(r *http.Request) (*SearchRequest, error) {
	limit, err := strconv.Atoi(r.URL.Query().Get("limit"))
	if err != nil {
		return nil, err
	}

	offset, err := strconv.Atoi(r.URL.Query().Get("offset"))
	if err != nil {
		return nil, err
	}

	query := r.URL.Query().Get("query")
	orderField := r.URL.Query().Get("order_field")

	orderBy, err := strconv.Atoi(r.URL.Query().Get("order_by"))
	if err != nil {
		return nil, err
	}

	searchRequest := SearchRequest{
		Limit:      limit,
		Offset:     offset,
		Query:      query,
		OrderField: orderField,
		OrderBy:    orderBy,
	}
	return &searchRequest, nil
}
