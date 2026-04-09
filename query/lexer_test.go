package query

import (
	"testing"
)

func TestNextToken(t *testing.T) {
	// The raw SQL strings we want to test
	input := `
		INSERT INTO users (id, name, age) VALUES ('101', 'Pratik', 22)
		SELECT * FROM users WHERE id = '101'
	`

	// The exact tokens we expect the lexer to output, in order
	tests := []struct {
		expectedType    TokenType
		expectedLiteral string
	}{
		// Testing: INSERT INTO users (id, name, age) VALUES ('101', 'Pratik', 22)
		{INSERT, "INSERT"},
		{INTO, "INTO"},
		{IDENT, "users"},
		{LPAREN, "("},
		{IDENT, "id"},
		{COMMA, ","},
		{IDENT, "name"},
		{COMMA, ","},
		{IDENT, "age"},
		{RPAREN, ")"},
		{VALUES, "VALUES"},
		{LPAREN, "("},
		{STRING, "101"},
		{COMMA, ","},
		{STRING, "Pratik"},
		{COMMA, ","},
		{NUMBER, "22"},
		{RPAREN, ")"},

		// Testing: SELECT * FROM users WHERE id = '101'
		{SELECT, "SELECT"},
		{ASTERISK, "*"},
		{FROM, "FROM"},
		{IDENT, "users"},
		{WHERE, "WHERE"},
		{IDENT, "id"},
		{ASSIGN, "="},
		{STRING, "101"},

		// End of file
		{EOF, ""},
	}

	l := NewLexer(input)

	// Loop through our expected tokens and compare them to what the lexer actually spits out
	for i, tt := range tests {
		tok := l.NextToken()

		if tok.Type != tt.expectedType {
			t.Fatalf("Test [%d] Failed - Token Type is wrong. expected=%q, got=%q",
				i, tt.expectedType, tok.Type)
		}

		if tok.Literal != tt.expectedLiteral {
			t.Fatalf("Test [%d] Failed - Literal is wrong. expected=%q, got=%q",
				i, tt.expectedLiteral, tok.Literal)
		}
	}
}
