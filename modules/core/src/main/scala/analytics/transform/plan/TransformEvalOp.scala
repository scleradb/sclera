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

package com.scleradb.analytics.transform.plan

import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.expr.{ScalExpr, SortExpr}
import com.scleradb.sql.plan.{RelEvalOp, RelEvalPlanResult}
import com.scleradb.sql.exec.ScalExprEvaluator
import com.scleradb.sql.result.TableResult

import com.scleradb.analytics.transform.expr.Transform
import com.scleradb.analytics.transform.datatypes.TransformTableResult

// generic transform
case class TransformEvalOp(
    evaluator: ScalExprEvaluator,
    transform: Transform
) extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val result: TableResult = TransformTableResult(
            evaluator, transform, inputResults.head.tableResult
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
