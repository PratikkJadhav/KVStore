package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type data struct {
	key   string
	value []byte
}

// type Object struct {
// 	data  data
// 	index int
// }

var store = make(map[string][]byte)

func Set(key string, value []byte) (resp string, err error) {
	store[key] = value
	return "OK", nil
}

func Get(key string) (value []byte) {
	value = store[key]

	return value
}

func main() {
	for {
		reader := bufio.NewReader(os.Stdin)
		input, _ := reader.ReadString('\n')
		input = strings.TrimSpace(input)

		parts := strings.Split(input, " ")
		cmd := parts[0]

		if cmd == "GET" {
			value := Get(parts[1])
			fmt.Println(string(value))
		} else if cmd == "SET" {
			resp, err := Set(parts[1], []byte(parts[2]))

			if err != nil {
				fmt.Println(err.Error())
			}
			fmt.Println(resp)
		}
	}
}
