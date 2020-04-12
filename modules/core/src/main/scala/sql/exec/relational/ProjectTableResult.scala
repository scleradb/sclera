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

import com.scleradb.sql.expr.{SortExpr, ScalColValue, ScalarTarget}
import com.scleradb.sql.types.SqlType
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}

/** Computes a projection on the input table result in-memory in a row stream
  * @param evaluator Scalar expression evaluator
  * @param targetExprs List of expressions to be computed, with the assigned
  *                    aliases, which become the output column names
  * @param inputResult Input result
  * @param resultOrderOpt Ordering of the rows in the result
  */
class ProjectTableResult(
    evaluator: ScalExprEvaluator,
    targetExprs: List[ScalarTarget],
    inputResult: TableResult,
    override val resultOrder: List[SortExpr]
) extends TableResult {
    override val columns: List[Column] = {
        // evaluate the targetExprs on a NULL valued row to get result types
        targetExprs.map { case target =>
            val t: SqlType =
                ScalTypeEvaluator.eval(target.expr, inputResult.columns)
            Column(target.alias.name, t.option)
        }
    }
    
    override def rows: Iterator[ScalTableRow] = inputResult.typedRows.map { t =>
        val valMap: List[(String, ScalColValue)] =
            targetExprs.map { target =>
                (target.alias.name -> evaluator.eval(target.expr, t))
            }

        ScalTableRow(valMap)
    }

    override def close(): Unit = { }
}

/** Companion object containing the constructor */
object ProjectTableResult {
    def apply(
        evaluator: ScalExprEvaluator,
        targetExprs: List[ScalarTarget],
        inputResult: TableResult,
        resultOrder: List[SortExpr]
    ): ProjectTableResult =
        new ProjectTableResult(evaluator, targetExprs, inputResult, resultOrder)
}
