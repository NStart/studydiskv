package main

import (
	"fmt"
	"regexp"
	"strings"
	"studydiskv"
)

var hex40 = regexp.MustCompile("[0-9a-fA-F]{40}")

func hexTransform(s string) *studydiskv.PathKey {
	if hex40.MatchString(s) {
		return &studydiskv.PathKey{
			Path:     []string{"objects", s[0:2]},
			FileName: s,
		}
	}
	folders := strings.Split(s, "/")
	lfolders := len(folders)
	if lfolders > 1 {
		return &studydiskv.PathKey{
			Path:     folders[:lfolders-1],
			FileName: folders[lfolders-1],
		}
	}
	return &studydiskv.PathKey{
		Path:     []string{},
		FileName: s,
	}
}

func hexInverseTransform(pathKey *studydiskv.PathKey) string {
	if hex40.MatchString(pathKey.FileName) {
		return pathKey.FileName
	}

	if len(pathKey.Path) == 0 {
		return pathKey.FileName
	}

	return strings.Join(pathKey.Path, "/") + "/" + pathKey.FileName
}

func main() {
	d := studydiskv.New(studydiskv.Options{
		BasePath:          "my-data-dir",
		AdvancedTransform: hexTransform,
		InverseTransform:  hexInverseTransform,
		CacheSizeMax:      1024 * 1024,
	})

	key := "1bd88421b055327fcc8660c76c4894c4ea4c95d7"
	d.WriteString(key, "¡Hola!")

	d.WriteString("refs/heads/master", "some text")

	fmt.Println("Enumerating All keys:")
	c := d.Keys(nil)

	for key := range c {
		value := d.ReadString(key)
		fmt.Printf("Key: %s, Value: %s\n", key, value)
	}
}
