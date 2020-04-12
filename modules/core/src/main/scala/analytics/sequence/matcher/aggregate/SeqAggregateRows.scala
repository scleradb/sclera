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

package com.scleradb.analytics.sequence.matcher.aggregate

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.expr._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.ScalTableRow
import com.scleradb.sql.exec.ScalExprEvaluator

sealed abstract class SeqAggregateRows {
    def result(curRow: ScalTableRow): List[List[ScalColValue]]
    def update(
        evaluator: ScalExprEvaluator,
        row: ScalTableRow,
        rowLabel: Label
    ): SeqAggregateRows
    val columns: List[Column]
}

case class SeqArgOpts(
    aggregates: List[SeqArgAggregate]
) extends SeqAggregateRows {
    // emit a row of NULLs if the aggregate does not exist
    override def result(curRow: ScalTableRow): List[List[ScalColValue]] =
        aggregates.flatMap { a => a.result }

    override def update(
        evaluator: ScalExprEvaluator,
        row: ScalTableRow,
        rowLabel: Label
    ): SeqArgOpts = {
        val updAggregates: List[SeqArgAggregate] =
            aggregates.map { a => a.update(evaluator, row, rowLabel) }

        if( updAggregates == aggregates ) this
        else SeqArgOpts(updAggregates)
    }

    override val columns: List[Column] = aggregates.head.columns
}

case class SeqAggregateColSet(
    aggregateCols: List[SeqAggregate],
    retainedCols: List[Column]
) extends SeqAggregateRows {
    override def result(curRow: ScalTableRow): List[List[ScalColValue]] = {
        val retainedColValues: List[ScalColValue] =
            retainedCols.map { col => curRow.getScalExpr(col) }
        val aggrValues: List[ScalColValue] =
            aggregateCols.flatMap { a => a.valueExprs }

        List(retainedColValues:::aggrValues)
    }

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqAggregateColSet = {
        val updAggregateCols: List[SeqAggregate] =
            aggregateCols.map { a => a.update(evaluator, r, rLabel) }

        if( updAggregateCols == aggregateCols ) this
        else SeqAggregateColSet(updAggregateCols, retainedCols)
    }

    override val columns: List[Column] =
        retainedCols:::aggregateCols.flatMap { a => a.columns }
}
