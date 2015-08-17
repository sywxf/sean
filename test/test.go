package main

import (
	"fmt"
	"sean/core"
)

func main() {
	engine, err := core.NewEngine("user=sywxf dbname=own sslmode=disable")
	if err != nil {
		fmt.Println(err)
	}
	a, _ := engine.GetTables()
	for _, v := range a {
		println(v.Name)
	}
	engine.GetColumns("typetest")
}
