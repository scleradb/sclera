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

package com.scleradb.sql.result

import com.scleradb.sql.expr.{ScalExpr, ScalColValue}
import com.scleradb.sql.exec.ScalExprEvaluator

/** Groups the consective result rows based on the group cols
  * @param evaluator Scalar expression evaluator
  * @param rows Iterator on table rows
  * @param groupExprs List of exprs to group on
  * @return Iterator on a TableGroup object containing the row groups
  */
class TableRowGroupIterator(
    evaluator: ScalExprEvaluator,
    rows: Iterator[ScalTableRow],
    groupExprs: List[ScalExpr]
) extends Iterator[TableRowGroup] {
    var rem: Iterator[ScalTableRow] = rows

    override def hasNext: Boolean = rem.hasNext

    override def next(): TableRowGroup = {
        val lookAhead: ScalTableRow = rem.next()
        val groupColValMap: Map[ScalExpr, ScalColValue] = Map() ++
            groupExprs.map { expr =>
                expr -> evaluator.eval(expr, lookAhead)
            }

        val (curIter, nextRem) = rem.span { row =>
            groupExprs.forall { expr =>
                evaluator.eval(expr, row) == groupColValMap(expr)
            }
        }

        rem = nextRem
        new TableRowGroup(groupColValMap, Iterator(lookAhead) ++ curIter)
    }
}

object TableRowGroupIterator {
    def apply(
        evaluator: ScalExprEvaluator,
        rows: Iterator[ScalTableRow],
        groupExprs: List[ScalExpr]
    ): TableRowGroupIterator =
        new TableRowGroupIterator(evaluator, rows, groupExprs)
}
