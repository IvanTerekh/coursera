package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"log"
	"net/http"
	"os"
	"text/template"
)

var (
	headTmpl = template.Must(template.New(`handler`).Parse(
		`package {{.Package}}

import (
	"net/http"	
)

`))
	handlerTmpl = template.Must(template.New(`handler`).Parse(
		`func (s *{{.Struct}}) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	
}
`))
)

type mytype string

func (s mytype) ServeHTTP(w http.ResponseWriter, r *http.Request) {

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


	for _, decl := range node.Decls {
		funcDecl, ok := decl.(*ast.FuncDecl)
		if !ok || funcDecl.Recv == nil {
			continue
		}

		starExpr, ok := funcDecl.Recv.List[0].Type.(*ast.StarExpr)
		if !ok {
			continue
		}

		indent, ok := starExpr.X.(*ast.Ident)
		if !ok {
			continue
		}

		err = handlerTmpl.Execute(out, struct {
			Struct string
		}{
			Struct: indent.Name,
		})
		if err != nil {
			log.Fatal(err)
		}
	}

}
