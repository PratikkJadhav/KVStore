package query

import "fmt"

type Parser struct {
	tokens    []Token
	pos       int
	curToken  Token
	peekToken Token
}

func NewParser(tokens []Token) *Parser {
	p := &Parser{
		tokens: tokens,
		pos:    -1, // start at -1 cause advance() will pointer to next ie 0
	}

	p.advance() //to make curToken =0 and  peektoken = tokens[0]
	p.advance() //to make curToken =tokens[0] and  peektoken = tokens[1]

	return p
}

func (p *Parser) advance() {
	p.pos++
	p.curToken = p.peekToken

	if p.pos < len(p.tokens) {
		p.peekToken = p.tokens[p.pos]
	} else {
		p.peekToken = Token{Type: EOF, Literal: ""}
	}
}

func (p *Parser) expect(t TokenType) error {
	if p.curToken.Type == t {
		p.advance()
		return nil
	}

	return fmt.Errorf("exprect %s but got %s (literal: %s)", t, p.curToken.Type, p.curToken.Literal)
}

func (p *Parser) parseSelect() (Statement, error) {
	p.advance()

	err := p.expect(ASTERISK)
	if err != nil {
		return nil, err
	}

	err = p.expect(FROM)
	if err != nil {
		return nil, err
	}

	if p.curToken.Type != IDENT {
		return nil, fmt.Errorf("Expected table name got %s", p.curToken.Type)
	}

	tableName := p.curToken.Literal
	p.advance()

	if p.curToken.Type != WHERE {
		return &SelectStmt{Table: tableName, ID: nil}, nil
	}
	p.advance()

	if p.curToken.Type != IDENT || p.curToken.Literal != "id" {
		return nil, fmt.Errorf("Expected 'id' after WHERE got '%s'", p.curToken.Type)
	}
	p.advance()

	err = p.expect(ASSIGN)
	if err != nil {
		return nil, err
	}

	if p.curToken.Type != STRING {
		return nil, fmt.Errorf("expected string value id , got %s", p.curToken.Type)
	}

	idvalue := p.curToken.Literal
	p.advance()

	return &SelectStmt{
		Table: tableName,
		ID:    &idvalue,
	}, nil
}

func (p *Parser) parseDelete() (Statement, error) {
	p.advance()

	if err := p.expect(FROM); err != nil {
		return nil, err
	}

	if p.curToken.Type != IDENT {
		return nil, fmt.Errorf("expected table name, got %s", p.curToken.Type)
	}
	tableName := p.curToken.Literal
	p.advance()

	if err := p.expect(WHERE); err != nil {
		return nil, err
	}

	if p.curToken.Type != IDENT || p.curToken.Literal != "id" {
		return nil, fmt.Errorf("expected 'id' after WHERE, got '%s'", p.curToken.Literal)
	}
	p.advance()
	if err := p.expect(ASSIGN); err != nil {
		return nil, err
	}

	if p.curToken.Type != STRING {
		return nil, fmt.Errorf("expected string id value, got %s", p.curToken.Type)
	}
	idValue := p.curToken.Literal
	p.advance() // consume the string value

	return &DeleteStmt{Table: tableName, ID: idValue}, nil
}

func (p *Parser) parseUpsert() (Statement, error) {
	p.advance()

	if err := p.expect(INTO); err != nil {
		return nil, err
	}

	if p.curToken.Type != IDENT {
		return nil, fmt.Errorf("expected table name, got %s", p.curToken.Type)
	}
	tableName := p.curToken.Literal
	p.advance() // consume table name

	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	columns, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}

	if err := p.expect(VALUES); err != nil {
		return nil, err
	}

	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	values, err := p.parseValueList()
	if err != nil {
		return nil, err
	}

	return &UpsertStmt{Table: tableName, Columns: columns, Values: values}, nil
}

func (p *Parser) parseIdentList() ([]string, error) {
	var list []string

	if p.curToken.Type == RPAREN {
		p.advance()
		return list, nil
	}

	if p.curToken.Type != IDENT {
		return nil, fmt.Errorf("expected IDENT, got %s", p.curToken.Type)
	}
	list = append(list, p.curToken.Literal)
	p.advance()

	for p.curToken.Type == COMMA {
		p.advance() // consume comma
		if p.curToken.Type != IDENT {
			return nil, fmt.Errorf("expected IDENT after comma, got %s", p.curToken.Type)
		}
		list = append(list, p.curToken.Literal)
		p.advance()
	}

	if err := p.expect(RPAREN); err != nil {
		return nil, err
	}

	return list, nil
}

func (p *Parser) parseValueList() ([]string, error) {
	var list []string

	if p.curToken.Type == RPAREN {
		p.advance()
		return list, nil
	}

	if p.curToken.Type != STRING && p.curToken.Type != NUMBER {
		return nil, fmt.Errorf("expected STRING or NUMBER, got %s", p.curToken.Type)
	}
	list = append(list, p.curToken.Literal)
	p.advance()

	for p.curToken.Type == COMMA {
		p.advance()
		if p.curToken.Type != STRING && p.curToken.Type != NUMBER {
			return nil, fmt.Errorf("expected STRING or NUMBER after comma, got %s", p.curToken.Type)
		}
		list = append(list, p.curToken.Literal)
		p.advance()
	}

	if err := p.expect(RPAREN); err != nil {
		return nil, err
	}

	return list, nil
}

func (p *Parser) parseInsert() (Statement, error) {
	p.advance() // consume 'INSERT'

	if err := p.expect(INTO); err != nil {
		return nil, err
	}

	if p.curToken.Type != IDENT {
		return nil, fmt.Errorf("expected table name, got %s", p.curToken.Type)
	}
	tableName := p.curToken.Literal
	p.advance()

	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	columns, err := p.parseIdentList()
	if err != nil {
		return nil, err
	}

	if err := p.expect(VALUES); err != nil {
		return nil, err
	}

	if err := p.expect(LPAREN); err != nil {
		return nil, err
	}
	values, err := p.parseValueList()
	if err != nil {
		return nil, err
	}

	return &InsertStmt{Table: tableName, Columns: columns, Values: values}, nil
}

func (p *Parser) Parse() (Statement, error) {
	switch p.curToken.Type {
	case SELECT:
		return p.parseSelect()
	case INSERT:
		return p.parseInsert()
	case DELETE:
		return p.parseDelete()
	case UPSERT:
		return p.parseUpsert()
	default:
		return nil, fmt.Errorf("unknown command: %s", p.curToken.Literal)
	}
}
