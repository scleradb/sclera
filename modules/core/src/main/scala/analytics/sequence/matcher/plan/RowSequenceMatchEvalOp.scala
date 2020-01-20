/**
* Sclera - Core
* Copyright 2012 - 2020 Sclera, Inc.
* 
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
* 
*     http://www.apache.org/licenses/LICENSE-2.0
* 
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

package com.scleradb.analytics.sequence.matcher.plan

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.dbms.location.LocationId

import com.scleradb.sql.expr.SortExpr
import com.scleradb.sql.result.{TableResult, ScalTableRow}
import com.scleradb.sql.plan.{RelEvalOp, RelEvalPlanResult}
import com.scleradb.sql.exec.ScalExprEvaluator

import com.scleradb.analytics.sequence.matcher.expr.LabeledMatch

private[scleradb]
case class RowSequenceMatchEvalOp(
    evaluator: ScalExprEvaluator,
    labels: ScalTableRow => List[Label],
    matchOp: LabeledMatch
) extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val result: TableResult =
            matchOp.matcher.matchResult(
                evaluator, labels, inputResults.head.tableResult
            )

        assert(
            SortExpr.isSubsumedBy(resOrder, result.resultOrder),
            "Unexpected sort order: " +
            result.resultOrder.map(s => s.expr.toString).mkString(", ") +
            " - expected: " +
            resOrder.map(s => s.expr.toString).mkString(", ")
        )

        RelEvalPlanResult(result)
    }
}
