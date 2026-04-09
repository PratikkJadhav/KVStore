package query

type TokenType string

const (
	EOF     TokenType = "EOF"
	ILLEGAL TokenType = "ILLEGAL"

	IDENT  TokenType = "IDENT"
	STRING TokenType = "STRING"
	NUMBER TokenType = "NUMBER"

	LPAREN   TokenType = "("
	RPAREN   TokenType = ")"
	COMMA    TokenType = ","
	ASSIGN   TokenType = "="
	ASTERISK TokenType = "*"

	INSERT TokenType = "INSERT"
	INTO   TokenType = "INTO"
	VALUES TokenType = "VALUES"
	SELECT TokenType = "SELECT"
	FROM   TokenType = "FROM"
	WHERE  TokenType = "WHERE"
	DELETE TokenType = "DELETE"
	UPSERT TokenType = "UPSERT"
)

type Token struct {
	Type    TokenType
	Literal string
}
