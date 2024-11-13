// Copied (with minor modifications) from https://github.com/prometheus
//
// Copyright 2019 The Prometheus Authors
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

%{
package parser

import (
        "github.com/prometheus/prometheus/model/labels"
)
%}

%union {
    node      Node
    item      Item
    matchers  []*labels.Matcher
    matcher   *labels.Matcher
    strings   []string
    float     float64
    duration  int64
    offsets   []int64
}


%token <item>
EQL
BLANK
COLON
COMMA
COMMENT
DOLLAR
DURATION
EOF
ERROR
IDENTIFIER
LEFT_BRACE
LEFT_BRACKET
LEFT_PAREN
METRIC_IDENTIFIER
NUMBER
RIGHT_BRACE
RIGHT_BRACKET
RIGHT_PAREN
SEMICOLON
SPACE
STRING
TIMES

// Operators.
%token	operatorsStart
%token <item>
ADD
BIND
DIV
EQLC
EQL_REGEX
GTE
GTR
LAND
LDEFAULT
LOR
LSS
LTE
LUNLESS
MOD
MUL
NEQ
NEQ_REGEX
POW
SUB
AT
ATAN2
%token	operatorsEnd

// Aggregators.
%token	aggregatorsStart
%token <item>
AVG
BOTTOMK
COUNT
COUNT_VALUES
DROP_EMPTY_SERIES
GROUP
MAX
MIN
QUANTILE
STDDEV
STDVAR
SUM
TOPK
SORT
SORT_DESC
AGGREGATE
%token	aggregatorsEnd

// Keywords.
%token	keywordsStart
%token <item>
BOOL
BY
GROUP_LEFT
GROUP_RIGHT
IGNORING
OFFSET
ON
WITHOUT
%token keywordsEnd

// Preprocessors.
%token preprocessorStart
%token <item>
START
END
%token preprocessorEnd


// Type definitions for grammar rules.
%type <matchers> label_match_list
%type <matcher> label_matcher

%type <item> aggregate_op grouping_label match_op maybe_label metric_identifier unary_op at_modifier_preprocessors

%type <strings> grouping_label_list grouping_labels maybe_grouping_labels
%type <float> number signed_number signed_or_unsigned_number
%type <node> step_invariant_expr aggregate_expr aggregate_modifier bin_modifier binary_expr bool_modifier expr function_call function_call_args function_call_body group_modifiers label_matchers matrix_selector number_literal offset_expr on_or_ignoring paren_expr string_literal unary_expr vector_selector
%type <duration> duration maybe_duration offset_value
%type <offsets> offset_list

%start start

// Operators are listed with increasing precedence.
%left LDEFAULT
%left LOR
%left LAND LUNLESS
%left EQLC GTE GTR LSS LTE NEQ
%left ADD SUB
%left MUL DIV MOD ATAN2
%right POW

// Offset modifiers do not have associativity.
%nonassoc OFFSET BIND

// This ensures that it is always attempted to parse range or subquery selectors when a left
// bracket is encountered.
%right LEFT_BRACKET

%%

start           :
                | /* empty */ EOF
                        { yylex.(*parser).addParseErrf(PositionRange{}, "no expression found in input")}
                | expr
                        { yylex.(*parser).generatedParserResult = $1 }
                | start EOF
                | error /* If none of the more detailed error messages are triggered, we fall back to this. */
                        { yylex.(*parser).unexpected("","") }
                ;

expr            :
                aggregate_expr
                | binary_expr
                | function_call
                | matrix_selector
                | number_literal
                | offset_expr
                | paren_expr
                | string_literal
                | unary_expr
                | vector_selector
                | step_invariant_expr
                ;

/*
 * Aggregations.
 */

aggregate_expr  : aggregate_op aggregate_modifier function_call_body
                        { $$ = yylex.(*parser).newAggregateExpr($1, $2, $3) }
                | aggregate_op function_call_body aggregate_modifier
                        { $$ = yylex.(*parser).newAggregateExpr($1, $3, $2) }
                | aggregate_op function_call_body
                        { $$ = yylex.(*parser).newAggregateExpr($1, &AggregateExpr{}, $2) }
                | aggregate_op error
                        {
                        yylex.(*parser).unexpected("aggregation","");
                        $$ = yylex.(*parser).newAggregateExpr($1, &AggregateExpr{}, Expressions{})
                        }
                ;

aggregate_modifier:
                BY grouping_labels
                        {
                        $$ = &AggregateExpr{
                                Grouping: $2,
                        }
                        }
                | WITHOUT grouping_labels
                        {
                        $$ = &AggregateExpr{
                                Grouping: $2,
                                Without:  true,
                        }
                        }
                ;

/*
 * Binary expressions.
 */

// Operator precedence only works if each of those is listed separately.
binary_expr     : expr ADD      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr ATAN2    bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr DIV      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr EQLC     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr GTE      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr GTR      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LAND     bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LOR      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LSS      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LTE      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LUNLESS  bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr MOD      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr MUL      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr NEQ      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr POW      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr SUB      bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                | expr LDEFAULT bin_modifier expr { $$ = yylex.(*parser).newBinaryExpression($1, $2, $3, $4) }
                ;

// Using left recursion for the modifier rules, helps to keep the parser stack small and
// reduces allocations
bin_modifier    : group_modifiers;

bool_modifier   : /* empty */
                        { $$ = &BinaryExpr{
                        VectorMatching: &VectorMatching{Card: CardOneToOne},
                        }
                        }
                | BOOL
                        { $$ = &BinaryExpr{
                        VectorMatching: &VectorMatching{Card: CardOneToOne},
                        ReturnBool:     true,
                        }
                        }
                ;

on_or_ignoring  : bool_modifier IGNORING grouping_labels
                        {
                        $$ = $1
                        $$.(*BinaryExpr).VectorMatching.MatchingLabels = $3
                        }
                | bool_modifier ON grouping_labels
                        {
                        $$ = $1
                        $$.(*BinaryExpr).VectorMatching.MatchingLabels = $3
                        $$.(*BinaryExpr).VectorMatching.On = true
                        }
                ;

group_modifiers: bool_modifier /* empty */
                | on_or_ignoring /* empty */
                | on_or_ignoring GROUP_LEFT maybe_grouping_labels
                        {
                        $$ = $1
                        $$.(*BinaryExpr).VectorMatching.Card = CardManyToOne
                        $$.(*BinaryExpr).VectorMatching.Include = $3
                        }
                | on_or_ignoring GROUP_RIGHT maybe_grouping_labels
                        {
                        $$ = $1
                        $$.(*BinaryExpr).VectorMatching.Card = CardOneToMany
                        $$.(*BinaryExpr).VectorMatching.Include = $3
                        }
                ;


grouping_labels : LEFT_PAREN grouping_label_list RIGHT_PAREN
                        { $$ = $2 }
                | LEFT_PAREN grouping_label_list COMMA RIGHT_PAREN
                        { $$ = $2 }
                | LEFT_PAREN RIGHT_PAREN
                        { $$ = []string{} }
                | error
                        { yylex.(*parser).unexpected("grouping opts", "\"(\""); $$ = nil }
                ;


grouping_label_list:
                grouping_label_list COMMA grouping_label
                        { $$ = append($1, $3.Val) }
                | grouping_label
                        { $$ = []string{$1.Val} }
                | grouping_label_list error
                        { yylex.(*parser).unexpected("grouping opts", "\",\" or \")\""); $$ = $1 }
                ;

grouping_label  : maybe_label
                        {
                        if !isLabel($1.Val) {
                                yylex.(*parser).unexpected("grouping opts", "label")
                        }
                        $$ = $1
                        }
                | error
                        { yylex.(*parser).unexpected("grouping opts", "label"); $$ = Item{} }
                ;

/*
 * Function calls.
 */

function_call   : IDENTIFIER function_call_body
                        {
                        fn, exist := getFunction($1.Val)
                        if !exist{
                                yylex.(*parser).addParseErrf($1.PositionRange(),"unknown function with name %q", $1.Val)
                        }
                        $$ = &Call{
                                Func: fn,
                                Args: $2.(Expressions),
                                PosRange: PositionRange{
                                        Start: $1.Pos,
                                        End:   yylex.(*parser).lastClosing,
                                },
                        }
                        }
                ;

function_call_body: LEFT_PAREN function_call_args RIGHT_PAREN
                        { $$ = $2 }
                | LEFT_PAREN RIGHT_PAREN
                        {$$ = Expressions{}}
                ;

function_call_args: function_call_args COMMA expr
                        { $$ = append($1.(Expressions), $3.(Expr)) }
                | expr
                        { $$ = Expressions{$1.(Expr)} }
                | function_call_args COMMA
                        {
                        yylex.(*parser).addParseErrf($2.PositionRange(), "trailing commas not allowed in function call args")
                        $$ = $1
                        }
                ;

/*
 * Expressions inside parentheses.
 */

paren_expr      : LEFT_PAREN expr RIGHT_PAREN
                        { $$ = &ParenExpr{Expr: $2.(Expr), PosRange: mergeRanges(&$1, &$3)} }
                ;

/*
 * Offset modifiers.
 */

offset_expr: expr OFFSET LEFT_BRACKET offset_list RIGHT_BRACKET
                {
                        yylex.(*parser).addOffset($1, 0, $4)
                        $$ = $1
                }
                | expr OFFSET offset_value
                {
                        yylex.(*parser).addOffset($1, $3, nil)
                        $$ = $1
                }
                ;

offset_list: offset_list COMMA offset_value
                {
                        $$ = append($$, $3)
                }
                | offset_value
                {
                        $$ = append($$, $1)
                }
                ;

offset_value: duration
                {
                        $$ = $1
                }
                | SUB duration
                {
                        $$ = -$2
                }
                ;

/*
 * @ modifiers.
 */

step_invariant_expr: expr AT signed_or_unsigned_number
                        {
                        yylex.(*parser).setTimestamp($1, $3)
                        $$ = $1
                        }
                | expr AT at_modifier_preprocessors LEFT_PAREN RIGHT_PAREN
                        {
                        yylex.(*parser).setAtModifierPreprocessor($1, $3)
                        $$ = $1
                        }
                | expr AT error
                        { yylex.(*parser).unexpected("@", "timestamp"); $$ = $1 }
                ;

at_modifier_preprocessors: START | END;

/*
 * Subquery and range selectors.
 */

matrix_selector : expr LEFT_BRACKET duration maybe_duration RIGHT_BRACKET
                {
                        switch vs := $1.(type) {
                        case *VectorSelector:
                                var errMsg string
                                if vs.OriginalOffset != 0 || len(vs.OriginalOffsetEx) != 0 {
                                        errMsg = "no offset modifiers allowed before range"
                                } else if vs.Timestamp != nil {
                                        errMsg = "no @ modifiers allowed before range"
                                }
                                if errMsg != "" {
                                        errRange := mergeRanges(&$2, &$5)
                                        yylex.(*parser).addParseErrf(errRange, errMsg)
                                }
                                $$ = &MatrixSelector{
                                        VectorSelector: $1.(Expr),
                                        Range: $3,
                                        EndPos: yylex.(*parser).lastClosing,
                                }
                        default:
                                $$ = &SubqueryExpr{
                                        Expr:  $1.(Expr),
                                        Range: $3,
                                        Step:  $4,
                                        EndPos: yylex.(*parser).lastClosing,
                                }
                        }
                }
                ;

/*
 * Unary expressions.
 */

unary_expr      :
                /* gives the rule the same precedence as MUL. This aligns with mathematical conventions */
                unary_op expr %prec MUL
                        {
                        if nl, ok := $2.(*NumberLiteral); ok {
                                if $1.Typ == SUB {
                                        nl.Val *= -1
                                }
                                nl.PosRange.Start = $1.Pos
                                $$ = nl
                        } else {
                                $$ = &UnaryExpr{Op: $1.Typ, Expr: $2.(Expr), StartPos: $1.Pos}
                        }
                        }
                ;

/*
 * Vector selectors.
 */

vector_selector: metric_identifier label_matchers
                        {
                        vs := $2.(*VectorSelector)
                        vs.PosRange = mergeRanges(&$1, vs)
                        vs.Name = $1.Val
                        yylex.(*parser).assembleVectorSelector(vs)
                        $$ = vs
                        }
                | metric_identifier
                        {
                        vs := &VectorSelector{
                                Name: $1.Val,
                                LabelMatchers: []*labels.Matcher{},
                                PosRange: $1.PositionRange(),
                        }
                        yylex.(*parser).assembleVectorSelector(vs)
                        $$ = vs
                        }
                | label_matchers
                        {
                        vs := $1.(*VectorSelector)
                        yylex.(*parser).assembleVectorSelector(vs)
                        $$ = vs
                        }
                ;

label_matchers  : LEFT_BRACE label_match_list RIGHT_BRACE
                        {
                        $$ = &VectorSelector{
                                LabelMatchers: $2,
                                PosRange: mergeRanges(&$1, &$3),
                        }
                        }
                | LEFT_BRACE label_match_list COMMA RIGHT_BRACE
                        {
                        $$ = &VectorSelector{
                                LabelMatchers: $2,
                                PosRange: mergeRanges(&$1, &$4),
                        }
                        }
                | LEFT_BRACE RIGHT_BRACE
                        {
                        $$ = &VectorSelector{
                                LabelMatchers: []*labels.Matcher{},
                                PosRange: mergeRanges(&$1, &$2),
                        }
                        }
                ;

label_match_list: label_match_list COMMA label_matcher
                        {
                        if $1 != nil{
                                $$ = append($1, $3)
                        } else {
                                $$ = $1
                        }
                        }
                | label_matcher
                        { $$ = []*labels.Matcher{$1}}
                | label_match_list error
                        { yylex.(*parser).unexpected("label matching", "\",\" or \"}\""); $$ = $1 }
                ;

label_matcher   : AT IDENTIFIER match_op STRING
                        { $$ = yylex.(*parser).newLabelMatcherInternal($2, $3, $4);  }
                | IDENTIFIER BIND DOLLAR IDENTIFIER
                        { $$ = yylex.(*parser).newVariableBinding($1, $4);  }
                | IDENTIFIER match_op STRING
                        { $$ = yylex.(*parser).newLabelMatcher($1, $2, $3);  }
                | IDENTIFIER match_op error
                        { yylex.(*parser).unexpected("label matching", "string"); $$ = nil}
                | IDENTIFIER error
                        { yylex.(*parser).unexpected("label matching", "label matching operator"); $$ = nil }
                | error
                        { yylex.(*parser).unexpected("label matching", "identifier or \"}\""); $$ = nil}
                ;

/*
 * Metric descriptions.
 */

metric_identifier: AVG | BOTTOMK | BY | COUNT | COUNT_VALUES | DROP_EMPTY_SERIES | GROUP | IDENTIFIER |  LAND | LOR | LUNLESS | MAX | METRIC_IDENTIFIER | MIN | OFFSET | QUANTILE | STDDEV | STDVAR | SUM | TOPK | SORT | SORT_DESC | WITHOUT | START | END;

/*
 * Keyword lists.
 */

aggregate_op    : AVG | BOTTOMK | COUNT | COUNT_VALUES | DROP_EMPTY_SERIES | GROUP | MAX | MIN | QUANTILE | STDDEV | STDVAR | SUM | TOPK | SORT | SORT_DESC | AGGREGATE;

// inside of grouping options label names can be recognized as keywords by the lexer. This is a list of keywords that could also be a label name.
maybe_label     : AVG | BOOL | BOTTOMK | BY | COUNT | COUNT_VALUES | DROP_EMPTY_SERIES | GROUP | GROUP_LEFT | GROUP_RIGHT | IDENTIFIER | IGNORING | LAND | LOR | LUNLESS | MAX | METRIC_IDENTIFIER | MIN | OFFSET | ON | QUANTILE | STDDEV | STDVAR | SUM | TOPK | SORT | SORT_DESC | START | END | ATAN2 | NUMBER;

unary_op        : ADD | SUB;

match_op        : EQL | NEQ | EQL_REGEX | NEQ_REGEX ;

/*
 * Literals.
 */

number_literal  : NUMBER
                        {
                        $$ = &NumberLiteral{
                                Val:           yylex.(*parser).number($1.Val),
                                PosRange: $1.PositionRange(),
                        }
                        }
                ;

number          : NUMBER { $$ = yylex.(*parser).number($1.Val) } ;

signed_number   : ADD number { $$ = $2 }
                | SUB number { $$ = -$2 }
                ;

signed_or_unsigned_number: number | signed_number ;

duration        : DURATION
                        {
                        var err error
                        $$, err = parseDuration($1.Val)
                        if err != nil {
                                yylex.(*parser).addParseErr($1.PositionRange(), err)
                        }
                        }
                ;


string_literal  : STRING
                        {
                        $$ = &StringLiteral{
                                Val: yylex.(*parser).unquoteString($1.Val),
                                PosRange: $1.PositionRange(),
                        }
                        }
                        ;

/*
 * Wrappers for optional arguments.
 */

maybe_duration  : /* empty */
                        {$$ = 0}
                | COLON {$$ = 0}
                | COLON duration {$$ = 1}
                ;

maybe_grouping_labels: /* empty */ { $$ = nil }
                | grouping_labels
                ;

%%
