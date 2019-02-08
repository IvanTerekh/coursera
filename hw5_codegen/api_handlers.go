package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

type response struct {
	Err      string      `json:"error"`
	Response interface{} `json:"response"`
}

func Error(w http.ResponseWriter, err error, code int) {
	http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), code)
}

func postMethodMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, `{"error":"bad method"}`, http.StatusNotAcceptable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("X-Auth")
		if auth != "100500" {
			http.Error(w, `{"error":"unauthorized"}`, http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func (srv *MyApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch path {

	case "/user/profile":
		handler := http.Handler(http.HandlerFunc(srv.HandlerProfile))


		handler.ServeHTTP(w, r)
	case "/user/create":
		handler := http.Handler(http.HandlerFunc(srv.HandlerCreate))
		handler = authMiddleware(handler)
		handler = postMethodMiddleware(handler)
		handler.ServeHTTP(w, r)
	default:
		http.Error(w, `{"error":"unknown method"}`, http.StatusNotFound)
	}
}

func (srv *MyApi) HandlerProfile(w http.ResponseWriter, r *http.Request) {
	params, err := parseProfileParams(r)
	if err != nil {
		Error(w, err, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	result, err := srv.Profile(ctx, *params)
	if err != nil {
		switch err.(type) {
		case ApiError:
			apiErr := err.(ApiError)
			Error(w, apiErr.Err, apiErr.HTTPStatus)
		default:
			Error(w, err, http.StatusInternalServerError)
		}
		return
	}

	resp := response{Response: result}
	respJson, err := json.Marshal(resp)
	if err != nil {
		log.Printf("could not marshal response: %#v", resp)
	}

	_, err = w.Write(respJson)
	if err != nil {
		panic(err)
	}
}

func parseProfileParams(r *http.Request) (*ProfileParams, error) {
	err := r.ParseForm()
	if err != nil {
		panic(err)
	}
	params := &ProfileParams{

		Login: r.Form.Get("login"),

	}




	if params.Login == "" {
		return nil, errors.New("login must me not empty")
	}





	return params, nil
}
func (srv *MyApi) HandlerCreate(w http.ResponseWriter, r *http.Request) {
	params, err := parseCreateParams(r)
	if err != nil {
		Error(w, err, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	result, err := srv.Create(ctx, *params)
	if err != nil {
		switch err.(type) {
		case ApiError:
			apiErr := err.(ApiError)
			Error(w, apiErr.Err, apiErr.HTTPStatus)
		default:
			Error(w, err, http.StatusInternalServerError)
		}
		return
	}

	resp := response{Response: result}
	respJson, err := json.Marshal(resp)
	if err != nil {
		log.Printf("could not marshal response: %#v", resp)
	}

	_, err = w.Write(respJson)
	if err != nil {
		panic(err)
	}
}

func parseCreateParams(r *http.Request) (*CreateParams, error) {
	err := r.ParseForm()
	if err != nil {
		panic(err)
	}
	params := &CreateParams{

		Login: r.Form.Get("login"),

		Name: r.Form.Get("full_name"),

		Status: r.Form.Get("status"),


	}

	Age, err := strconv.Atoi(r.Form.Get("age"))
	if err != nil {
		return nil, errors.New("age must be int")
	}
	params.Age = Age




	if params.Login == "" {
		return nil, errors.New("login must me not empty")
	}




	if len(params.Login) < 10 {
		return nil, errors.New("login len must be >= 10")
	}











	if params.Status == "" {
		params.Status = "user"
	}



	if  params.Status != "user" &&
		 params.Status != "moderator" &&
		 params.Status != "admin" {
		return nil, errors.New("status must be one of [user, moderator, admin]")
	}









	if params.Age < 0 {
		return nil, errors.New("age must be >= 0")
	}



	if params.Age > 128 {
		return nil, errors.New("age must be <= 128")
	}


	return params, nil
}
func (srv *OtherApi) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch path {

	case "/user/create":
		handler := http.Handler(http.HandlerFunc(srv.HandlerCreate))
		handler = authMiddleware(handler)
		handler = postMethodMiddleware(handler)
		handler.ServeHTTP(w, r)
	default:
		http.Error(w, `{"error":"unknown method"}`, http.StatusNotFound)
	}
}

func (srv *OtherApi) HandlerCreate(w http.ResponseWriter, r *http.Request) {
	params, err := parseOtherCreateParams(r)
	if err != nil {
		Error(w, err, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	result, err := srv.Create(ctx, *params)
	if err != nil {
		switch err.(type) {
		case ApiError:
			apiErr := err.(ApiError)
			Error(w, apiErr.Err, apiErr.HTTPStatus)
		default:
			Error(w, err, http.StatusInternalServerError)
		}
		return
	}

	resp := response{Response: result}
	respJson, err := json.Marshal(resp)
	if err != nil {
		log.Printf("could not marshal response: %#v", resp)
	}

	_, err = w.Write(respJson)
	if err != nil {
		panic(err)
	}
}

func parseOtherCreateParams(r *http.Request) (*OtherCreateParams, error) {
	err := r.ParseForm()
	if err != nil {
		panic(err)
	}
	params := &OtherCreateParams{

		Username: r.Form.Get("username"),

		Name: r.Form.Get("account_name"),

		Class: r.Form.Get("class"),


	}

	Level, err := strconv.Atoi(r.Form.Get("level"))
	if err != nil {
		return nil, errors.New("level must be int")
	}
	params.Level = Level




	if params.Username == "" {
		return nil, errors.New("username must me not empty")
	}




	if len(params.Username) < 3 {
		return nil, errors.New("username len must be >= 3")
	}











	if params.Class == "" {
		params.Class = "warrior"
	}



	if  params.Class != "warrior" &&
		 params.Class != "sorcerer" &&
		 params.Class != "rouge" {
		return nil, errors.New("class must be one of [warrior, sorcerer, rouge]")
	}









	if params.Level < 1 {
		return nil, errors.New("level must be >= 1")
	}



	if params.Level > 50 {
		return nil, errors.New("level must be <= 50")
	}


	return params, nil
}