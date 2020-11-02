package scanner

import (
	"fmt"
	"unicode/utf8"

	"github.com/xqueries/xdb/internal/parser/scanner/ruleset"
	"github.com/xqueries/xdb/internal/parser/scanner/token"
)

var _ Scanner = (*ruleBasedScanner)(nil)

type ruleBasedScanner struct {
	input []rune

	cache token.Token

	whitespaceDetector ruleset.DetectorFunc
	linefeedDetector   ruleset.DetectorFunc
	rules              []ruleset.Rule

	singleLineCommentDetector ruleset.DetectorFunc

	state
}

type state struct {
	start     int
	startLine int
	startCol  int
	pos       int
	line      int
	col       int
}

// NewRuleBased creates a new, ready to use rule based scanner with the given
// ruleset, that will process the given input rune slice.
func NewRuleBased(input string, ruleset ruleset.Ruleset) (Scanner, error) {
	if !utf8.ValidString(input) {
		return nil, ErrInvalidUTF8String
	}

	return &ruleBasedScanner{
		input:              []rune(input),
		cache:              nil,
		whitespaceDetector: ruleset.WhitespaceDetector,
		linefeedDetector:   ruleset.LinefeedDetector,
		rules:              ruleset.Rules,
		state: state{
			start:     0,
			startLine: 1,
			startCol:  1,
			pos:       0,
			line:      1,
			col:       1,
		},
		singleLineCommentDetector: ruleset.SingleLineCommentDetector,
	}, nil
}

// Next returns the next token and removes it from the scanner.
// The next call to Next() or Peek() will compute a new token, which
// follows the token that this method returned.
func (s *ruleBasedScanner) Next() token.Token {
	tok := s.Peek()
	s.cache = nil
	return tok
}

// Peek returns the next token without removing it. To remove it,
// call Next().
func (s *ruleBasedScanner) Peek() token.Token {
	if s.cache == nil {
		s.cache = s.computeNext()
	}
	return s.cache
}

func (s *ruleBasedScanner) checkpoint() state {
	return s.state
}

func (s *ruleBasedScanner) restore(chck state) {
	s.state = chck
}

func (s *ruleBasedScanner) done() bool {
	return s.pos >= len(s.input)
}

func (s *ruleBasedScanner) computeNext() token.Token {
	s.drainWhitespacesAndComments()

	if s.done() {
		return s.eof()
	}
	return s.applyRule()
}

func (s *ruleBasedScanner) applyRule() token.Token {
	// try to apply all rules in the given order
	for _, rule := range s.rules {
		chck := s.checkpoint()
		typ, ok := rule.Apply(s)
		if ok {
			return s.token(typ)
		}
		s.restore(chck)
	}

	// no rules matched, create an error token
	s.ConsumeRune() // skip the one offending rune
	return s.unexpectedToken()
}

func (s *ruleBasedScanner) drainWhitespacesAndComments() {
	for {
		s.drainWhitespace()
		if !s.drainComment() {
			return
		}
	}
}

func (s *ruleBasedScanner) drainWhitespace() {
	for {
		next, ok := s.Lookahead()
		if !(ok && (s.whitespaceDetector(next) || s.linefeedDetector(next))) {
			break
		}
		s.ConsumeRune()
	}
	_ = s.token(token.Unknown) // discard consumed tokens
}

// drainComment will check for comments and discards them
// Using: https://www.sqlite.org/lang_comment.html
func (s *ruleBasedScanner) drainComment() bool {
	chkPoint := s.checkpoint()

	singleLineCmt := func(s *ruleBasedScanner) bool {
		first, ok := s.Lookahead()
		if ok && s.singleLineCommentDetector(first) {
			s.ConsumeRune()
			next, ok := s.Lookahead()
			if ok && next == first {
				for {
					s.ConsumeRune()
					next, ok := s.Lookahead()
					if !ok {
						return true
					}
					if s.linefeedDetector(next) {
						s.ConsumeRune()
						return true
					}
				}
			}
		}
		return false
	}
	multiLineCmt := func(s *ruleBasedScanner) bool {
		first, ok := s.Lookahead()
		if ok && first == '/' {
			s.ConsumeRune()
			next, ok := s.Lookahead()
			if ok && next == '*' {
				s.ConsumeRune()
				for {
					next, ok := s.Lookahead()
					if !ok {
						return true
					}
					s.ConsumeRune()
					if next == '*' {
						upNext, ok := s.Lookahead()
						if !ok || upNext == '/' {
							s.ConsumeRune()
							return true
						}
					}
				}
			}
		}
		return false
	}

	found := false
	for {
		notFound := 0
		if !singleLineCmt(s) {
			notFound++
			s.restore(chkPoint)
		} else {
			found = true
			chkPoint = s.checkpoint()
		}
		if !multiLineCmt(s) {
			notFound++
			s.restore(chkPoint)
		} else {
			found = true
			chkPoint = s.checkpoint()
		}
		if notFound == 2 {
			break
		}
	}
	_ = s.token(token.Unknown) // discard consumed tokens
	return found
}

func (s *ruleBasedScanner) candidate() string {
	return string(s.input[s.start:s.pos])
}

func (s *ruleBasedScanner) eof() token.Token {
	return s.token(token.EOF)
}

func (s *ruleBasedScanner) unexpectedToken() token.Token {
	return s.errorToken(fmt.Errorf("%w: '%v' at offset %v", ErrUnexpectedToken, s.candidate(), s.start))
}

func (s *ruleBasedScanner) token(t token.Type) token.Token {
	tok := token.New(s.startLine, s.startCol, s.start, s.pos-s.start, t, s.candidate())
	s.updateStartPositions()
	return tok
}

func (s *ruleBasedScanner) errorToken(err error) token.Token {
	tok := token.NewErrorToken(s.startLine, s.startCol, s.start, s.pos-s.start, token.Error, err)
	s.updateStartPositions()
	return tok
}

func (s *ruleBasedScanner) updateStartPositions() {
	s.start = s.pos
	s.startLine = s.line
	s.startCol = s.col
}

// runeScanner

// Lookahead returns the next rune that is ahead of the current position, or false,
// if EOF was reached.
func (s *ruleBasedScanner) Lookahead() (rune, bool) {
	if !s.done() {
		return s.input[s.pos], true
	}
	return 0, false
}

// ConsumeRune adds the next rune, that can be obtained with Lookahead(), to the
// current candidate string. This also advances the position pointer by 1 and adapts
// the line and col pointer according to whether the rune was a linefeed or not.
func (s *ruleBasedScanner) ConsumeRune() {
	if s.linefeedDetector(s.input[s.pos]) {
		s.line++
		s.col = 1
	} else {
		s.col++
	}
	s.pos++
}
