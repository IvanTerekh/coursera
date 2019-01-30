package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
)

type user struct {
	Name     string   `json:"name"`
	Email    string   `json:"email"`
	Browsers []string `json:"browsers"`
}

// вам надо написать более быструю оптимальную этой функции
func FastSearch(out io.Writer) {
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}

	r := strings.NewReplacer("@", " [at] ")
	seenBrowsers := make(map[string]bool)

	fmt.Fprintln(out, "found users:")

	scanner := bufio.NewScanner(file)
	for id := 0; scanner.Scan(); id++ {
		user := new(user)
		err := user.UnmarshalJSON(scanner.Bytes())
		if err != nil {
			panic(err)
		}

		isAndroid := false
		isMSIE := false

		for _, browser := range user.Browsers {
			if strings.Contains(browser, "Android") {
				isAndroid = true
				if !seenBrowsers[browser] {
					// log.Printf("SLOW New browser: %s, first seen: %s", browser, user["name"])
					seenBrowsers[browser] = true
				}
			}

			if strings.Contains(browser, "MSIE") {
				isMSIE = true
				if !seenBrowsers[browser] {
					// log.Printf("SLOW New browser: %s, first seen: %s", browser, user["name"])
					seenBrowsers[browser] = true
				}
			}
		}

		if !(isAndroid && isMSIE) {
			continue
		}

		// log.Println("Android and MSIE user:", user["name"], user["email"])
		email := r.Replace(user.Email)
		fmt.Fprintf(out, "[%d] %s <%s>\n", id, user.Name, email)
	}

	fmt.Fprintln(out)
	fmt.Fprintln(out, "Total unique browsers", len(seenBrowsers))
}
