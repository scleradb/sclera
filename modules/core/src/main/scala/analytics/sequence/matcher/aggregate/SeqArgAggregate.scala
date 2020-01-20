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

import com.scleradb.sql.expr.{ScalExpr, ScalColValue, ScalValueBase, SqlNull}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.ScalTableRow
import com.scleradb.sql.exec.ScalExprEvaluator

private[scleradb]
sealed abstract class SeqArgAggregate extends java.io.Serializable {
    val columns: List[Column]
    def result: List[List[ScalColValue]]
    def update(
        evaluator: ScalExprEvaluator,
        row: ScalTableRow,
        rowLabel: Label
    ): SeqArgAggregate
}

private[scleradb]
case class SeqLabeledArgAggregate(
    aggregate: SeqUnLabeledArgAggregate,
    labels: List[Label]
) extends SeqArgAggregate {
    override val columns: List[Column] = aggregate.columns

    override def result: List[List[ScalColValue]] = aggregate.result

    override def update(
        evaluator: ScalExprEvaluator,
        row: ScalTableRow,
        rowLabel: Label
    ): SeqLabeledArgAggregate =
        if( labels contains rowLabel )
            SeqLabeledArgAggregate(
                aggregate.update(evaluator, row, rowLabel), labels
            )
        else this
}

private[scleradb]
case class SeqUnLabeledArgAggregate(
    param: ScalExpr,
    override val columns: List[Column],
    isRunningOpt: (Boolean, ScalValueBase, ScalValueBase) => Boolean,
    isAllRows: Boolean = true,
    runningOpt: Option[ScalValueBase] = None,
    optRowsVals: List[List[ScalColValue]] = Nil
) extends SeqArgAggregate {
    override def result: List[List[ScalColValue]] = optRowsVals.reverse

    override def update(
        evaluator: ScalExprEvaluator,
        row: ScalTableRow,
        rowLabel: Label
    ): SeqUnLabeledArgAggregate = {
        val vOpt: Option[ScalValueBase] =
            evaluator.eval(param, row) match {
                case (_: SqlNull) => None
                case (v: ScalValueBase) => Some(v)
            }

        val (isOpt, isEqual) = (runningOpt, vOpt) match {
            case (_, None) => (false, false)
            case (None, _) => (true, false)
            case (Some(m), Some(v)) => (isRunningOpt(isAllRows, m, v), m == v)
        }

        if( isOpt ) {
            val curRowVals: List[ScalColValue] =
                columns.map { col => row.getScalExpr(col) }
            val updOptRowVals: List[List[ScalColValue]] =
                if( isEqual ) curRowVals::optRowsVals else List(curRowVals)
            SeqUnLabeledArgAggregate(
                param, columns, isRunningOpt, isAllRows, vOpt, updOptRowVals
            )
        } else this
    }
}

private[scleradb]
object SeqArgAggregate {
    def apply(
        columns: List[Column],
        baseAggregate: SeqAggregate
    ): SeqArgAggregate = baseAggregate match {
        case (aggregate: SeqOptAggregate) =>
            SeqUnLabeledArgAggregate(
                aggregate.param, columns, aggregate.isRunningOpt
            )

        case SeqLabeledAggregate(aggregate: SeqOptAggregate, Nil) =>
            SeqUnLabeledArgAggregate(
                aggregate.param, columns, aggregate.isRunningOpt
            )

        case SeqLabeledAggregate(aggregate: SeqOptAggregate, labels) =>
            SeqLabeledArgAggregate(
                SeqUnLabeledArgAggregate(
                    aggregate.param, columns, aggregate.isRunningOpt
                ),
                labels
            )

        case _ =>
            throw new IllegalArgumentException(
                "Cannot evaluate ARG for the specified aggregates"
            )
    }
}
