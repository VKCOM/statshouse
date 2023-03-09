// Copied (with minor modifications) from https://github.com/prometheus
//
// Copyright 2015 The Prometheus Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//go:generate go run golang.org/x/tools/cmd/goyacc@latest -o parse.y.go parse.y

package parser

import (
	"errors"
	"fmt"
	"math"
	"os"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/timestamp"
	"github.com/prometheus/prometheus/util/strutil"
)

var parserPool = sync.Pool{
	New: func() interface{} {
		return &parser{}
	},
}

type parser struct {
	lex Lexer

	inject    ItemType
	injecting bool

	// Everytime an Item is lexed that could be the end
	// of certain expressions its end position is stored here.
	lastClosing Pos

	yyParser yyParserImpl

	generatedParserResult interface{}
	parseErrors           ParseErrors
}

// ParseErr wraps a parsing error with line and position context.
type ParseErr struct {
	PositionRange PositionRange
	Err           error
	Query         string

	// LineOffset is an additional line offset to be added. Only used inside unit tests.
	LineOffset int
}

func (e *ParseErr) Error() string {
	pos := int(e.PositionRange.Start)
	lastLineBreak := -1
	line := e.LineOffset + 1

	var positionStr string

	if pos < 0 || pos > len(e.Query) {
		positionStr = "invalid position:"
	} else {

		for i, c := range e.Query[:pos] {
			if c == '\n' {
				lastLineBreak = i
				line++
			}
		}

		col := pos - lastLineBreak
		positionStr = fmt.Sprintf("%d:%d:", line, col)
	}
	return fmt.Sprintf("%s parse error: %s", positionStr, e.Err)
}

type ParseErrors []ParseErr

// Since producing multiple error messages might look weird when combined with error wrapping,
// only the first error produced by the parser is included in the error string.
// If getting the full error list is desired, it is recommended to typecast the error returned
// by the parser to ParseErrors and work with the underlying slice.
func (errs ParseErrors) Error() string {
	if len(errs) != 0 {
		return errs[0].Error()
	}
	// Should never happen
	// Panicking while printing an error seems like a bad idea, so the
	// situation is explained in the error message instead.
	return "error contains no error message"
}

// ParseExpr returns the expression parsed from the input.
func ParseExpr(input string) (expr Expr, err error) {
	p := newParser(input)
	defer parserPool.Put(p)
	defer p.recover(&err)

	parseResult := p.parseGenerated(START_EXPRESSION)

	if parseResult != nil {
		expr = parseResult.(Expr)
	}

	if len(p.parseErrors) != 0 {
		err = p.parseErrors
	}

	return expr, err
}

// newParser returns a new parser.
func newParser(input string) *parser {
	p := parserPool.Get().(*parser)

	p.injecting = false
	p.parseErrors = nil
	p.generatedParserResult = nil

	// Clear lexer struct before reusing.
	p.lex = Lexer{
		input: input,
		state: lexStatements,
	}
	return p
}

// SequenceValue is an omittable value in a sequence of time series values.
type SequenceValue struct {
	Value   float64
	Omitted bool
}

func (v SequenceValue) String() string {
	if v.Omitted {
		return "_"
	}
	return fmt.Sprintf("%f", v.Value)
}

type seriesDescription struct {
	labels labels.Labels
	values []SequenceValue
}

// addParseErrf formats the error and appends it to the list of parsing errors.
func (p *parser) addParseErrf(positionRange PositionRange, format string, args ...interface{}) {
	p.addParseErr(positionRange, fmt.Errorf(format, args...))
}

// addParseErr appends the provided error to the list of parsing errors.
func (p *parser) addParseErr(positionRange PositionRange, err error) {
	perr := ParseErr{
		PositionRange: positionRange,
		Err:           err,
		Query:         p.lex.input,
	}

	p.parseErrors = append(p.parseErrors, perr)
}

// unexpected creates a parser error complaining about an unexpected lexer item.
// The item that is presented as unexpected is always the last item produced
// by the lexer.
func (p *parser) unexpected(context, expected string) {
	var errMsg strings.Builder

	// Do not report lexer errors twice
	if p.yyParser.lval.item.Typ == ERROR {
		return
	}

	errMsg.WriteString("unexpected ")
	errMsg.WriteString(p.yyParser.lval.item.desc())

	if context != "" {
		errMsg.WriteString(" in ")
		errMsg.WriteString(context)
	}

	if expected != "" {
		errMsg.WriteString(", expected ")
		errMsg.WriteString(expected)
	}

	p.addParseErr(p.yyParser.lval.item.PositionRange(), errors.New(errMsg.String()))
}

var errUnexpected = errors.New("unexpected error")

// recover is the handler that turns panics into returns from the top level of Parse.
func (p *parser) recover(errp *error) {
	e := recover()
	if _, ok := e.(runtime.Error); ok {
		// Print the stack trace but do not inhibit the running application.
		buf := make([]byte, 64<<10)
		buf = buf[:runtime.Stack(buf, false)]

		fmt.Fprintf(os.Stderr, "parser panic: %v\n%s", e, buf)
		*errp = errUnexpected
	} else if e != nil {
		*errp = e.(error)
	}
}

// Lex is expected by the yyLexer interface of the yacc generated parser.
// It writes the next Item provided by the lexer to the provided pointer address.
// Comments are skipped.
//
// The yyLexer interface is currently implemented by the parser to allow
// the generated and non-generated parts to work together with regards to lookahead
// and error handling.
//
// For more information, see https://pkg.go.dev/golang.org/x/tools/cmd/goyacc.
func (p *parser) Lex(lval *yySymType) int {
	var typ ItemType

	if p.injecting {
		p.injecting = false
		return int(p.inject)
	}
	// Skip comments.
	for {
		p.lex.NextItem(&lval.item)
		typ = lval.item.Typ
		if typ != COMMENT {
			break
		}
	}

	switch typ {
	case ERROR:
		pos := PositionRange{
			Start: p.lex.start,
			End:   Pos(len(p.lex.input)),
		}
		p.addParseErr(pos, errors.New(p.yyParser.lval.item.Val))

		// Tells yacc that this is the end of input.
		return 0
	case EOF:
		lval.item.Typ = EOF
		p.InjectItem(0)
	case RIGHT_BRACE, RIGHT_PAREN, RIGHT_BRACKET, DURATION, NUMBER:
		p.lastClosing = lval.item.Pos + Pos(len(lval.item.Val))
	}

	return int(typ)
}

// Error is expected by the yyLexer interface of the yacc generated parser.
//
// It is a no-op since the parsers error routines are triggered
// by mechanisms that allow more fine-grained control
// For more information, see https://pkg.go.dev/golang.org/x/tools/cmd/goyacc.
func (p *parser) Error(e string) {
}

// InjectItem allows injecting a single Item at the beginning of the token stream
// consumed by the generated parser.
// This allows having multiple start symbols as described in
// https://www.gnu.org/software/bison/manual/html_node/Multiple-start_002dsymbols.html .
// Only the Lex function used by the generated parser is affected by this injected Item.
// Trying to inject when a previously injected Item has not yet been consumed will panic.
// Only Item types that are supposed to be used as start symbols are allowed as an argument.
func (p *parser) InjectItem(typ ItemType) {
	if p.injecting {
		panic("cannot inject multiple Items into the token stream")
	}

	if typ != 0 && (typ <= startSymbolsStart || typ >= startSymbolsEnd) {
		panic("cannot inject symbol that isn't start symbol")
	}

	p.inject = typ
	p.injecting = true
}

func (p *parser) newBinaryExpression(lhs Node, op Item, modifiers, rhs Node) *BinaryExpr {
	ret := modifiers.(*BinaryExpr)

	ret.LHS = lhs.(Expr)
	ret.RHS = rhs.(Expr)
	ret.Op = op.Typ

	return ret
}

func (p *parser) assembleVectorSelector(vs *VectorSelector) {
	if vs.Name != "" {
		nameMatcher, err := labels.NewMatcher(labels.MatchEqual, labels.MetricName, vs.Name)
		if err != nil {
			panic(err) // Must not happen with labels.MatchEqual
		}
		vs.LabelMatchers = append(vs.LabelMatchers, nameMatcher)
	}
}

func (p *parser) newAggregateExpr(op Item, modifier, args Node) (ret *AggregateExpr) {
	ret = modifier.(*AggregateExpr)
	arguments := args.(Expressions)

	ret.PosRange = PositionRange{
		Start: op.Pos,
		End:   p.lastClosing,
	}

	ret.Op = op.Typ

	if len(arguments) == 0 {
		p.addParseErrf(ret.PositionRange(), "no arguments for aggregate expression provided")

		// Prevents invalid array accesses.
		return
	}

	desiredArgs := 1
	if ret.Op.IsAggregatorWithParam() {
		desiredArgs = 2

		ret.Param = arguments[0]
	}

	if len(arguments) != desiredArgs {
		p.addParseErrf(ret.PositionRange(), "wrong number of arguments for aggregate expression provided, expected %d, got %d", desiredArgs, len(arguments))
		return
	}

	ret.Expr = arguments[desiredArgs-1]

	return ret
}

// number parses a number.
func (p *parser) number(val string) float64 {
	n, err := strconv.ParseInt(val, 0, 64)
	f := float64(n)
	if err != nil {
		f, err = strconv.ParseFloat(val, 64)
	}
	if err != nil {
		p.addParseErrf(p.yyParser.lval.item.PositionRange(), "error parsing number: %s", err)
	}
	return f
}

func (p *parser) unquoteString(s string) string {
	unquoted, err := strutil.Unquote(s)
	if err != nil {
		p.addParseErrf(p.yyParser.lval.item.PositionRange(), "error unquoting string %q: %s", s, err)
	}
	return unquoted
}

func parseDuration(ds string) (int64, error) {
	dur, err := model.ParseDuration(ds)
	if err != nil {
		return 0, err
	}
	if dur == 0 {
		return 0, errors.New("duration must be greater than 0")
	}
	return int64(math.Round(float64(dur) / float64(time.Second))), nil
}

// parseGenerated invokes the yacc generated parser.
// The generated parser gets the provided startSymbol injected into
// the lexer stream, based on which grammar will be used.
func (p *parser) parseGenerated(startSymbol ItemType) interface{} {
	p.InjectItem(startSymbol)

	p.yyParser.Parse(p)

	return p.generatedParserResult
}

func (p *parser) newLabelMatcher(label, operator, value Item) *labels.Matcher {
	op := operator.Typ
	val := p.unquoteString(value.Val)

	// Map the Item to the respective match type.
	var matchType labels.MatchType
	switch op {
	case EQL:
		matchType = labels.MatchEqual
	case NEQ:
		matchType = labels.MatchNotEqual
	case EQL_REGEX:
		matchType = labels.MatchRegexp
	case NEQ_REGEX:
		matchType = labels.MatchNotRegexp
	default:
		// This should never happen, since the error should have been caught
		// by the generated parser.
		panic("invalid operator")
	}

	m, err := labels.NewMatcher(matchType, label.Val, val)
	if err != nil {
		p.addParseErr(mergeRanges(&label, &value), err)
	}

	return m
}

// addOffset is used to set the offset in the generated parser.
func (p *parser) addOffset(e Node, offset int64) {
	var orgoffsetp *int64
	var endPosp *Pos

	switch s := e.(type) {
	case *VectorSelector:
		orgoffsetp = &s.OriginalOffset
		endPosp = &s.PosRange.End
	case *MatrixSelector:
		vs, ok := s.VectorSelector.(*VectorSelector)
		if !ok {
			p.addParseErrf(e.PositionRange(), "ranges only allowed for vector selectors")
			return
		}
		orgoffsetp = &vs.OriginalOffset
		endPosp = &s.EndPos
	case *SubqueryExpr:
		orgoffsetp = &s.OriginalOffset
		endPosp = &s.EndPos
	default:
		p.addParseErrf(e.PositionRange(), "offset modifier must be preceded by an instant vector selector or range vector selector or a subquery")
		return
	}

	// it is already ensured by parseDuration func that there never will be a zero offset modifier
	if *orgoffsetp != 0 {
		p.addParseErrf(e.PositionRange(), "offset may not be set multiple times")
	} else if orgoffsetp != nil {
		*orgoffsetp = offset
	}

	*endPosp = p.lastClosing
}

// setTimestamp is used to set the timestamp from the @ modifier in the generated parser.
func (p *parser) setTimestamp(e Node, ts float64) {
	if math.IsInf(ts, -1) || math.IsInf(ts, 1) || math.IsNaN(ts) ||
		ts >= float64(math.MaxInt64) || ts <= float64(math.MinInt64) {
		p.addParseErrf(e.PositionRange(), "timestamp out of bounds for @ modifier: %f", ts)
	}
	var timestampp **int64
	var endPosp *Pos

	timestampp, _, endPosp, ok := p.getAtModifierVars(e)
	if !ok {
		return
	}

	if timestampp != nil {
		*timestampp = new(int64)
		**timestampp = timestamp.FromFloatSeconds(ts)
	}

	*endPosp = p.lastClosing
}

// setAtModifierPreprocessor is used to set the preprocessor for the @ modifier.
func (p *parser) setAtModifierPreprocessor(e Node, op Item) {
	_, preprocp, endPosp, ok := p.getAtModifierVars(e)
	if !ok {
		return
	}

	if preprocp != nil {
		*preprocp = op.Typ
	}

	*endPosp = p.lastClosing
}

func (p *parser) getAtModifierVars(e Node) (**int64, *ItemType, *Pos, bool) {
	var (
		timestampp **int64
		preprocp   *ItemType
		endPosp    *Pos
	)
	switch s := e.(type) {
	case *VectorSelector:
		timestampp = &s.Timestamp
		preprocp = &s.StartOrEnd
		endPosp = &s.PosRange.End
	case *MatrixSelector:
		vs, ok := s.VectorSelector.(*VectorSelector)
		if !ok {
			p.addParseErrf(e.PositionRange(), "ranges only allowed for vector selectors")
			return nil, nil, nil, false
		}
		preprocp = &vs.StartOrEnd
		timestampp = &vs.Timestamp
		endPosp = &s.EndPos
	case *SubqueryExpr:
		preprocp = &s.StartOrEnd
		timestampp = &s.Timestamp
		endPosp = &s.EndPos
	default:
		p.addParseErrf(e.PositionRange(), "@ modifier must be preceded by an instant vector selector or range vector selector or a subquery")
		return nil, nil, nil, false
	}

	if *timestampp != nil || (*preprocp) == START || (*preprocp) == END {
		p.addParseErrf(e.PositionRange(), "@ <timestamp> may not be set multiple times")
		return nil, nil, nil, false
	}

	return timestampp, preprocp, endPosp, true
}
