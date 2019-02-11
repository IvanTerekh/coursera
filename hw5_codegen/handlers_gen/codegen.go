package main

import (
	"encoding/json"
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"text/template"
)

var (
	headTmpl = template.Must(template.New(`headTmpl`).Parse(
		`package {{.Package}}

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
)

type response struct {
	Err      string      ` + "`" + `json:"error"` + "`" + `
	Response interface{} ` + "`" + `json:"response"` + "`" + `
}

func Error(w http.ResponseWriter, err error, code int) {
	http.Error(w, fmt.Sprintf(` + "`" + `{"error":"%s"}` + "`" + `, err.Error()), code)
}

func postMethodMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, ` + "`" + `{"error":"bad method"}` + "`" + `, http.StatusNotAcceptable)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func authMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth := r.Header.Get("X-Auth")
		if auth != "100500" {
			http.Error(w, ` + "`" + `{"error":"unauthorized"}` + "`" + `, http.StatusForbidden)
			return
		}

		next.ServeHTTP(w, r)
	})
}
`))

	ServeHTTPTmpl = template.Must(template.New(`serveHTTP`).Parse(`
func (srv *{{.ApiName}}) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	switch path { {{range .Methods}}
	case "{{.Url}}":
		handler := http.Handler(http.HandlerFunc(srv.Handler{{.Name}}))
{{if .Auth}}		handler = authMiddleware(handler){{end}}
{{if eq .Method "POST"}}		handler = postMethodMiddleware(handler){{end}}
		handler.ServeHTTP(w, r){{end}}
	default:
		http.Error(w, ` + "`" + `{"error":"unknown method"}` + "`" + `, http.StatusNotFound)
	}
}
`))

	Handler = template.Must(template.New(`Handler`).Parse(`
func (srv *{{.Api}}) Handler{{.Name}}(w http.ResponseWriter, r *http.Request) {
	params, err := parse{{.ParamsName}}(r)
	if err != nil {
		Error(w, err, http.StatusBadRequest)
		return
	}

	ctx := r.Context()
	result, err := srv.{{.Name}}(ctx, *params)
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
`))

	ParseParams = template.Must(template.New(`ParseParams`).Parse(`
func parse{{.ParamsName}}(r *http.Request) (*{{.ParamsName}}, error) {
	err := r.ParseForm()
	if err != nil {
		panic(err)
	}
	params := &{{.ParamsName}}{
{{range .Params}}{{if eq .TypeName "string"}}
		{{.Name}}: r.Form.Get("{{.ParamName}}"),{{end}}
{{end}}
	}
{{range .Params}}{{if eq .TypeName "int"}}
	{{.Name}}, err := strconv.Atoi(r.Form.Get("{{.ParamName}}"))
	if err != nil {
		return nil, errors.New("{{.ParamName}} must be int")
	}
	params.{{.Name}} = {{.Name}}
{{end}}{{end}}
{{range .Params}}
{{if .DefaultVal}}
	{{if eq .TypeName "int"}}
	if params.{{.Name}} == 0 {
		params.{{.Name}} = {{.DefaultVal}}
	}
	{{else}}
	if params.{{.Name}} == "" {
		params.{{.Name}} = "{{.DefaultVal}}"
	}
	{{end}}
{{end}}
{{if .Required}}
	if params.{{.Name}} == "" {
		return nil, errors.New("{{.ParamName}} must me not empty")
	}
{{end}}
{{if .Enum}}{{$name := .Name}}
	if {{range $index, $element := .Enum}}{{if $index}}&&
		{{end}} params.{{$name}} != "{{$element}}" {{end}}{
		return nil, errors.New("{{.ParamName}} must be one of [{{range $index, $element := .Enum}}{{if $index}}, {{end}}{{$element}}{{end}}]")
	}
{{end}}
{{if .Min}}
{{if eq .TypeName "string"}}
	if len(params.{{.Name}}) < {{.MinVal}} {
		return nil, errors.New("{{.ParamName}} len must be >= {{.MinVal}}")
	}
{{else}}
	if params.{{.Name}} < {{.MinVal}} {
		return nil, errors.New("{{.ParamName}} must be >= {{.MinVal}}")
	}
{{end}}
{{end}}
{{if .Max}}
	if params.{{.Name}} > {{.MaxVal}} {
		return nil, errors.New("{{.ParamName}} must be <= {{.MaxVal}}")
	}
{{end}}
{{end}}
	return params, nil
}
`))
)

type apiMethod struct {
	Name       string
	Api        string
	ParamsName string
	Params     []param
	Method     string `json:"method"`
	Auth       bool   `json:"auth"`
	Url        string `json:"url"`
}

type typeName int

const (
	stringType = iota
	intType
)

type param struct {
	Name       string
	TypeName   string
	Required   bool
	ParamName  string
	Enum       []string
	DefaultVal string
	Min        bool
	MinVal     int
	Max        bool
	MaxVal     int
}

func main() {
	fset := token.NewFileSet()
	node, err := parser.ParseFile(fset, os.Args[1], nil, parser.ParseComments)
	if err != nil {
		log.Fatal(err)
	}

	out, err := os.Create(os.Args[2])
	if err != nil {
		log.Fatal(err)
	}

	err = headTmpl.Execute(out, struct {
		Package string
	}{
		Package: node.Name.Name,
	})
	if err != nil {
		log.Fatal(err)
	}

	apis := extractApis(node)
	for apiName, methods := range apis {
		err = ServeHTTPTmpl.Execute(out, struct {
			ApiName string
			Methods []apiMethod
		}{
			ApiName: apiName,
			Methods: methods,
		})
		if err != nil {
			log.Println(err)
		}

		for _, method := range methods {
			err = Handler.Execute(out, method)
			if err != nil {
				log.Println(err)
			}

			err = ParseParams.Execute(out, method)
			if err != nil {
				log.Println(err)
			}
		}
	}

	err = out.Close()
	if err != nil {
		log.Println(err)
	}
	err = exec.Command("go", "fmt", os.Args[2]).Run()
	if err != nil {
		log.Println(err)
	}

}

func extractApis(node *ast.File) map[string][]apiMethod {
	paramsMap := make(map[string][]param)
	var apis = make(map[string][]apiMethod)
	for _, decl := range node.Decls {
		switch decl.(type) {
		case *ast.FuncDecl:
			funcDecl, ok := decl.(*ast.FuncDecl)
			if !ok || funcDecl.Recv == nil {
				continue
			}

			if funcDecl.Doc == nil {
				continue
			}
			comment := funcDecl.Doc.List[0].Text
			if !strings.HasPrefix(comment, "// apigen:api ") {
				continue
			}

			config := strings.TrimPrefix(comment, "// apigen:api ")
			method := new(apiMethod)
			err := json.Unmarshal([]byte(config), method)
			if err != nil {
				log.Println(err)
			}

			paramsName := funcDecl.Type.Params.List[1].Type.(*ast.Ident).Name
			method.Params = paramsMap[paramsName]
			method.Name = funcDecl.Name.Name
			method.ParamsName = paramsName

			starExpr, ok := funcDecl.Recv.List[0].Type.(*ast.StarExpr)
			if !ok {
				continue
			}

			indent, ok := starExpr.X.(*ast.Ident)
			if !ok {
				continue
			}
			api := indent.Name
			method.Api = api
			apis[api] = append(apis[api], *method)
		case *ast.GenDecl:
			genDecl, ok := decl.(*ast.GenDecl)
			if !ok {
				continue
			}

			for _, spec := range genDecl.Specs {
				typeSpec, ok := spec.(*ast.TypeSpec)
				if !ok {
					continue
				}

				name := typeSpec.Name.Name
				if !strings.HasSuffix(name, "Params") {
					continue
				}

				structType, ok := typeSpec.Type.(*ast.StructType)
				if !ok {
					continue
				}

				params := extractParams(structType.Fields)
				paramsMap[name] = params
			}
		}
	}

	return apis
}

func extractParams(fields *ast.FieldList) []param {
	var params []param

	for _, field := range fields.List {
		param := param{
			Name: field.Names[0].Name,
		}
		param.ParamName = strings.ToLower(param.Name)

			typeSpec, ok := field.Type.(*ast.Ident)
		if !ok {
			continue
		}
		param.TypeName = typeSpec.Name

		configs := strings.Split(strings.TrimSuffix(strings.TrimPrefix(
			field.Tag.Value, "`apivalidator:\""), "\"`"), ",")
		for _, config := range configs {
			var key, val string
			sep := strings.IndexByte(config, '=')
			if sep > 0 {
				val = strings.TrimSuffix(config[sep+1:], "\"`")
				key = config[:sep]
			} else {
				key = config
			}
			switch key {
			case "required":
				param.Required = true
			case "paramname":
				param.ParamName = val
			case "enum":
				param.Enum = strings.Split(val, "|")
			case "default":
				param.DefaultVal = val
				//switch param.TypeName {
				//case "string":
				//case "int":
				//	intVal, err := strconv.Atoi(val)
				//	if err != nil {
				//		log.Println(err)
				//	}
				//	param.DefaultVal = intVal
				//}
			case "min":
				param.Min = true
				min, err := strconv.Atoi(val)
				if err != nil {
					log.Println(err)
					continue
				}
				param.MinVal = min
			case "max":
				param.Max = true
				max, err := strconv.Atoi(val)
				if err != nil {
					log.Println(err)
					continue
				}
				param.MaxVal = max
			}
		}
		params = append(params, param)
	}

	return params
}
