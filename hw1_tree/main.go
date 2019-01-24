package main

import (
	"fmt"
	"io"
	"os"
	"sort"
)

func main() {
	out := os.Stdout
	if !(len(os.Args) == 2 || len(os.Args) == 3) {
		panic("usage go run main.go . [-f]")
	}
	path := os.Args[1]
	printFiles := len(os.Args) == 3 && os.Args[2] == "-f"
	err := dirTree(out, path, printFiles)
	if err != nil {
		panic(err.Error())
	}
}

func dirTree(writer io.Writer, dir string, printFiles bool) error {
	return dirTreeRec(writer, dir, printFiles, "")
}

func dirTreeRec(writer io.Writer, dir string, printFiles bool, prefix string) error {
	files, err := getFiles(dir, !printFiles)
	if err != nil {
		return err
	}

	filesNumber := len(files)
	if filesNumber == 0 {
		return nil
	}

	for i := 0; i < filesNumber-1; i++ {
		writeSubtree(writer, prefix, dir, files[i], printFiles, false)
	}
	writeSubtree(writer, prefix, dir, files[filesNumber-1], printFiles, true)

	return nil
}

func getFiles(dir string, dirOnly bool) ([]os.FileInfo, error) {
	file, err := os.Open(dir)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	var files []os.FileInfo
	if fileInfo.IsDir() {
		files, err = file.Readdir(0)
		if err != nil {
			return nil, err
		}

		if dirOnly {
			filtered := make([]os.FileInfo, 0, len(files))
			for _, file := range files {
				if file.IsDir() {
					filtered = append(filtered, file)
				}
			}
			files = filtered
		}

		sort.Slice(files, func(i, j int) bool { return files[i].Name() < files[j].Name() })
	}
	return files, nil
}

func writeSubtree(writer io.Writer, prefix, dir string, file os.FileInfo, printFiles, last bool) error {
	var newPrefix string
	if !last {
		newPrefix = prefix + "│\t"
		prefix += "├───"
	} else {
		newPrefix = prefix + "\t"
		prefix += "└───"
	}

	var sizeStr string
	if !file.IsDir() {
		var size = file.Size()
		if size == 0 {
			sizeStr = " (empty)"
		} else {
			sizeStr = fmt.Sprintf(" (%db)", size)
		}
	}

	_, err := writer.Write([]byte(prefix + file.Name() + sizeStr + "\n"))
	if err != nil {
		return err
	}

	err = dirTreeRec(writer, dir+string(os.PathSeparator)+file.Name(), printFiles, newPrefix)
	return err
}
