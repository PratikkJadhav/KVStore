package query

import (
	"testing"
)

// Helper function to tokenize a string before parsing
func tokenizeInput(input string) []Token {
	l := NewLexer(input)
	var tokens []Token
	for {
		tok := l.NextToken()
		tokens = append(tokens, tok)
		if tok.Type == EOF {
			break
		}
	}
	return tokens
}

func TestParseInsert(t *testing.T) {
	input := `INSERT INTO users (id, name, age) VALUES ('101', 'Pratik', 22)`
	tokens := tokenizeInput(input)
	p := NewParser(tokens)

	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() returned an error: %v", err)
	}

	// Type assertion: Make sure it actually returned an InsertStmt
	insertStmt, ok := stmt.(*InsertStmt)
	if !ok {
		t.Fatalf("stmt is not *InsertStmt. got=%T", stmt)
	}

	if insertStmt.Table != "users" {
		t.Errorf("insertStmt.Table wrong. expected='users', got='%s'", insertStmt.Table)
	}

	if len(insertStmt.Columns) != 3 || insertStmt.Columns[0] != "id" {
		t.Errorf("Columns are wrong. got=%v", insertStmt.Columns)
	}

	if len(insertStmt.Values) != 3 || insertStmt.Values[1] != "Pratik" {
		t.Errorf("Values are wrong. got=%v", insertStmt.Values)
	}
}

func TestParseSelect(t *testing.T) {
	input := `SELECT * FROM users WHERE id = '101'`
	tokens := tokenizeInput(input)
	p := NewParser(tokens)

	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() returned an error: %v", err)
	}

	selectStmt, ok := stmt.(*SelectStmt)
	if !ok {
		t.Fatalf("stmt is not *SelectStmt. got=%T", stmt)
	}

	if selectStmt.Table != "users" {
		t.Errorf("selectStmt.Table wrong. expected='users', got='%s'", selectStmt.Table)
	}

	if selectStmt.ID == nil || *selectStmt.ID != "101" {
		t.Errorf("selectStmt.ID wrong. expected='101', got='%v'", selectStmt.ID)
	}
}

func TestParseDelete(t *testing.T) {
	input := `DELETE FROM users WHERE id = '101'`
	tokens := tokenizeInput(input)
	p := NewParser(tokens)

	stmt, err := p.Parse()
	if err != nil {
		t.Fatalf("Parse() returned an error: %v", err)
	}

	deleteStmt, ok := stmt.(*DeleteStmt)
	if !ok {
		t.Fatalf("stmt is not *DeleteStmt. got=%T", stmt)
	}

	if deleteStmt.Table != "users" {
		t.Errorf("deleteStmt.Table wrong. expected='users', got='%s'", deleteStmt.Table)
	}

	if deleteStmt.ID != "101" {
		t.Errorf("deleteStmt.ID wrong. expected='101', got='%s'", deleteStmt.ID)
	}
}
