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

import com.scleradb.sql.expr.{ColRef, ScalExpr, SortExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, TableRow, TableRowGroupIterator}

/** Computes DISTINCT or DISTINCT ON on the input
  * @param evaluator Scalar expression evaluator
  * @param exprs Expressions to group on (assuming the input is sorted on these)
  * @param limitOpt Limit (optional) within each group
  * @param offset Offset within each group
  * @param inputResult Streaming input on which the operator is evaluated
  */
private[scleradb]
class DistinctTableResult(
    evaluator: ScalExprEvaluator,
    exprs: List[ScalExpr],
    limitOpt: Option[Int],
    offset: Int,
    inputResult: TableResult
) extends TableResult {
    override val columns: List[Column] = inputResult.columns

    override val resultOrder: List[SortExpr] = inputResult.resultOrder

    override def rows: Iterator[TableRow] = {
        val groupIt: TableRowGroupIterator =
            TableRowGroupIterator(evaluator, inputResult.typedRows, exprs)

        limitOpt match {
            case Some(limit) =>
                groupIt.flatMap { group => group.rows.drop(offset).take(limit) }
            case None =>
                groupIt.flatMap { group => group.rows.drop(offset) }
        }
    }

    override def close(): Unit = { }
}

/** Companion object containing the constructor */
private[scleradb]
object DistinctTableResult {
    def apply(
        evaluator: ScalExprEvaluator,
        exprs: List[ScalExpr],
        limitOpt: Option[Int],
        offset: Int,
        inputResult: TableResult
    ): DistinctTableResult =
        new DistinctTableResult(evaluator, exprs, limitOpt, offset, inputResult)

    def apply(
        evaluator: ScalExprEvaluator,
        exprs: List[ScalExpr],
        inputResult: TableResult
    ): DistinctTableResult =
        apply(evaluator, exprs, Some(1), 0, inputResult)

    def apply(
        evaluator: ScalExprEvaluator,
        inputResult: TableResult
    ): DistinctTableResult = {
        val exprs: List[ColRef] =
            inputResult.columns.map { col => ColRef(col.name) }
        apply(evaluator, exprs, inputResult)
    }

    def apply(
        evaluator: ScalExprEvaluator,
        exprsOpt: Option[List[ScalExpr]],
        inputResult: TableResult
    ): DistinctTableResult = exprsOpt match {
        case Some(exprs) => apply(evaluator, exprs, inputResult)
        case None => apply(evaluator, inputResult)
    }
}
