package main

import (
	"fmt"
	"strings"
	"studydiskv"
)

func AdvancedTransformExample(key string) *studydiskv.PathKey {
	path := strings.Split(key, "/")
	last := len(path) - 1
	return &studydiskv.PathKey{
		Path:     path[:last],
		FileName: path[last] + ".txt",
	}
}

func InverseTransformExample(pathKey *studydiskv.PathKey) (key string) {
	txt := pathKey.FileName[len(pathKey.FileName)-4:]
	if txt != ".txt" {
		panic("Invalid file found in storage folder")
	}
	return strings.Join(pathKey.Path, "/") + pathKey.FileName[:len(pathKey.FileName)-4]
}

func main() {
	d := studydiskv.New(studydiskv.Options{
		BasePath:          "my-data-dir",
		AdvancedTransform: AdvancedTransformExample,
		InverseTransform:  InverseTransformExample,
		CacheSizeMax:      1024 * 1024,
	})

	key := "alpha/beta/gamma"
	d.WriteString(key, "¡Hola!")
	fmt.Println(d.ReadString("alpha/beta/gamma"))
}
