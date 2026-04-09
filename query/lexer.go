package query

import (
	"unicode"
)

type lexer struct {
	input    string
	pos      int
	readPosi int
	ch       byte
}

var keywords = map[string]TokenType{
	"INSERT": INSERT,
	"INTO":   INTO,
	"VALUES": VALUES,
	"SELECT": SELECT,
	"FROM":   FROM,
	"WHERE":  WHERE,
	"DELETE": DELETE,
	"UPSERT": UPSERT,
}

func NewLexer(input string) *lexer {
	l := &lexer{input: input}
	l.readChar()
	return l
}

func (l *lexer) readChar() {
	if l.readPosi >= len(l.input) {
		l.ch = 0 // 0 is NULL in ASCII
	} else {
		l.ch = l.input[l.readPosi]
	}

	l.pos = l.readPosi
	l.readPosi++
}

func (l *lexer) NextToken() Token {
	var token Token

	l.skipWhitespace()

	switch l.ch {
	case '=':
		token = newToken(ASSIGN, l.ch)
	case '(':
		token = newToken(LPAREN, l.ch)
	case ')':
		token = newToken(RPAREN, l.ch)
	case '*':
		token = newToken(ASTERISK, l.ch)
	case ',':
		token = newToken(COMMA, l.ch)
	case '\'':
		token.Literal = l.readString()
		token.Type = STRING
		return token
	case 0:
		token.Literal = ""
		token.Type = EOF
	default:
		if unicode.IsLetter(rune(l.ch)) {
			ident := l.readIdentifier()
			token = l.lookupKeyword(ident)
			return token
		} else if unicode.IsDigit(rune(l.ch)) {
			num := l.readNumber()
			token = Token{Type: NUMBER, Literal: num}
			return token
		} else {
			token = newToken(ILLEGAL, l.ch)
		}
	}

	l.readChar()
	return token
}

func (l *lexer) skipWhitespace() {
	for l.ch == ' ' || l.ch == '\t' || l.ch == '\n' || l.ch == '\r' {
		l.readChar()
	}
}

func (l *lexer) readString() string {
	l.readChar()
	t := ""
	for l.ch != '\'' && l.ch != 0 {
		t += string(l.ch)
		l.readChar()
	}
	if l.ch == '\'' {
		l.readChar()
	}

	return t

}

func (l *lexer) readIdentifier() string {
	t := ""
	for unicode.IsLetter(rune(l.ch)) {
		t += string(l.ch)
		l.readChar()
	}
	return t
}
func (l *lexer) readNumber() string {
	t := ""

	for unicode.IsDigit(rune(l.ch)) {
		t += string(l.ch)
		l.readChar()
	}
	return t
}

func (l *lexer) lookupKeyword(ident string) Token {

	if tokType, ok := keywords[ident]; ok {
		return Token{Type: tokType, Literal: ident}
	}

	return Token{Type: IDENT, Literal: ident}
}
func newToken(tokenType TokenType, ch byte) Token {
	return Token{Type: tokenType, Literal: string(ch)}
}
