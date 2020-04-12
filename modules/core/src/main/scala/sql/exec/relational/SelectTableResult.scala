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

package com.scleradb.sql.exec

import com.scleradb.sql.expr.{ScalExpr, BoolConst, SortExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}

/** Filters rows basedd on the given predicate
  * @param evaluator Scalar expression evaluator
  * @param predExpr Predicate for filtering the input rows
  * @param inputResult Table result to be wrapped over
  */
class SelectTableResult(
    evaluator: ScalExprEvaluator,
    predExpr: ScalExpr,
    inputResult: TableResult
) extends TableResult {
    override val columns: List[Column] = inputResult.columns
    
    override val resultOrder: List[SortExpr] = inputResult.resultOrder

    override def rows: Iterator[ScalTableRow] =
        inputResult.typedRows.filter { t =>
            evaluator.eval(predExpr, t) match {
                case BoolConst(true) => true
                case _ => false
            }
        }

    override def close(): Unit = { }
}

/** Companion object containing the constructor */
object SelectTableResult {
    def apply(
        evaluator: ScalExprEvaluator,
        predExpr: ScalExpr,
        inpResult: TableResult
    ): SelectTableResult = new SelectTableResult(evaluator, predExpr, inpResult)
}
