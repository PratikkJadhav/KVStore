package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/PratikkJadhav/KVStore.git/bitcask"
	"github.com/PratikkJadhav/KVStore.git/query"
)

func main() {
	bc, err := bitcask.Open()
	if err != nil {
		panic(err)
	}
	reader := bufio.NewReader(os.Stdin)

	executor := query.NewExecutor(bc)

	for {
		fmt.Print("> ")

		input, _ := reader.ReadString('\n')

		input = strings.TrimSpace(input)

		if err == io.EOF && input == "" {
			fmt.Println("\nEOF")
			break
		}

		if input == "" {
			continue
		}

		lexer := query.NewLexer(input)
		var tokens []query.Token
		for {
			tok := lexer.NextToken()
			tokens = append(tokens, tok)
			if tok.Type == query.EOF {
				break
			}
		}

		parser := query.NewParser(tokens)
		stmt, err := parser.Parse()
		if err != nil {
			fmt.Println("Parse error:", err)
			continue
		}

		result, err := executor.Execute(stmt)
		if err != nil {
			fmt.Println("Error:", err)
		} else {
			fmt.Println(result)
		}
	}
}
