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

type Statement interface {
	statementNode()
}

type InsertStmt struct {
	Table   string
	Columns []string
	Values  []string
}

type SelectStmt struct {
	Table string
	ID    *string
}

type DeleteStmt struct {
	Table string
	ID    string
}

type UpsertStmt struct {
	Table   string
	Columns []string
	Values  []string
}

func (i *InsertStmt) statementNode() {}
func (i *SelectStmt) statementNode() {}
func (i *DeleteStmt) statementNode() {}
func (i *UpsertStmt) statementNode() {}
