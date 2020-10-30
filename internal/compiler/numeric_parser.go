package compiler

import (
	"bytes"
	"strings"
)

type numericParserState func(*numericParser) numericParserState

type numericParser struct {
	candidate string
	index     int

	isReal                  bool
	isHexadecimal           bool
	isErronous              bool
	hasDigitsBeforeExponent bool

	value *bytes.Buffer

	current numericParserState
}

// isNumeric is an adapted copy of (engine.Engine).ToNumericValue.
func isNumeric(s string) bool {
	if s == "" || s == "0x" || s == "." {
		return false
	}
	p := numericParser{
		candidate: s,
		index:     0,

		value: &bytes.Buffer{},

		current: stateInitial,
	}
	p.parse()
	return !p.isErronous
}

func (p numericParser) done() bool {
	return p.index >= len(p.candidate)
}

func (p *numericParser) parse() {
	for p.current != nil && !p.done() {
		p.current = p.current(p)
	}
}

func (p *numericParser) get() byte {
	return p.candidate[p.index]
}

func (p *numericParser) step() {
	_ = p.value.WriteByte(p.get())
	p.index++
}

func stateInitial(p *numericParser) numericParserState {
	switch {
	case strings.HasPrefix(p.candidate, "0x"):
		p.index += 2
		p.isHexadecimal = true
		return stateHex
	case isDigit(p.get()):
		return stateFirstDigits
	case p.get() == '.':
		return stateDecimalPoint
	}
	p.isErronous = true
	return nil
}

func stateHex(p *numericParser) numericParserState {
	if isHexDigit(p.get()) {
		p.step()
		return stateHex
	}
	p.isErronous = true
	return nil
}

func stateFirstDigits(p *numericParser) numericParserState {
	if isDigit(p.get()) {
		p.hasDigitsBeforeExponent = true
		p.step()
		return stateFirstDigits
	} else if p.get() == '.' {
		return stateDecimalPoint
	}
	p.isErronous = true
	return nil
}

func stateDecimalPoint(p *numericParser) numericParserState {
	if p.get() == '.' {
		p.step()
		p.isReal = true
		return stateSecondDigits
	}
	p.isErronous = true
	return nil
}

func stateSecondDigits(p *numericParser) numericParserState {
	if isDigit(p.get()) {
		p.hasDigitsBeforeExponent = true
		p.step()
		return stateSecondDigits
	} else if p.get() == 'E' {
		if p.hasDigitsBeforeExponent {
			return stateExponent
		}
		p.isErronous = true // if there were no first digits,
	}
	p.isErronous = true
	return nil
}

func stateExponent(p *numericParser) numericParserState {
	if p.get() == 'E' {
		p.step()
		return stateOptionalSign
	}
	p.isErronous = true
	return nil
}

func stateOptionalSign(p *numericParser) numericParserState {
	if p.get() == '+' || p.get() == '-' {
		p.step()
		return stateThirdDigits
	} else if isDigit(p.get()) {
		return stateThirdDigits
	}
	p.isErronous = true
	return nil
}

func stateThirdDigits(p *numericParser) numericParserState {
	if isDigit(p.get()) {
		p.step()
		return stateThirdDigits
	}
	p.isErronous = true
	return nil
}

func isDigit(b byte) bool {
	return b-'0' <= 9
}

func isHexDigit(b byte) bool {
	return isDigit(b) || (b-'A' <= 15) || (b-'a' <= 15)
}
