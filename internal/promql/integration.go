// Copyright 2022 V Kontakte LLC
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.

package promql

import (
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

// Exported private promql functions
// Extracted to separate file to change the source code of prometheus less
// (it is easier to see what's really changed)

func UnwrapParenExpr(e *parser.Expr) {
	unwrapParenExpr(e)
}

func (ng *Engine) GetTimeRangesForSelector(s *parser.EvalStmt, n *parser.VectorSelector, path []parser.Node, evalRange time.Duration) (int64, int64) {
	start, end := ng.getTimeRangesForSelector(s, n, path, evalRange)
	return start / 1_000, end / 1_000
}
