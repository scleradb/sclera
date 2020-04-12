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

package com.scleradb.analytics.sequence.matcher

import scala.collection.mutable

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.exec.ScalExprEvaluator
import com.scleradb.sql.expr.{ColRef, ScalColValue, SqlNull, SortExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}
import com.scleradb.sql.result.TableRowGroupIterator

import com.scleradb.analytics.sequence.matcher.aggregate._

class LabelSequenceMatchResult(
    evaluator: ScalExprEvaluator,
    isEmptyResultAllowed: Boolean,
    partnColRefs: List[ColRef],
    isPartnColsInResult: Boolean,
    aggregateRowsSpec: SeqAggregateRowsSpec,
    rowLabels: ScalTableRow => List[Label],
    input: TableResult,
    override val resultOrder: List[SortExpr]
) extends TableResult {
    private val aggregates: SeqAggregateRows =
        aggregateRowsSpec.aggregate(input)

    private val partnCols: List[Column] = partnColRefs.map { colRef =>
        input.columnOpt(colRef.name).getOrElse {
            throw new IllegalArgumentException(
                "Partition column \"" + colRef.repr + "\" not found"
            )
        }
    }

    override val columns: List[Column] =
        if( isPartnColsInResult ) {
            partnCols.map { col =>
                Column(col.name, col.sqlType.option)
            } ::: aggregates.columns
        } else aggregates.columns

    private val subPartnCols: List[ColRef] =
        SortExpr.compatiblePartnCols(input.resultOrder, partnColRefs)

    override def rows: Iterator[ScalTableRow] = {
        val inpRows: Iterator[ScalTableRow] = input.typedRows
        if( inpRows.hasNext ) {
            if( subPartnCols.isEmpty ) rows(inpRows) else {
                TableRowGroupIterator(
                    evaluator, inpRows, subPartnCols
                ).flatMap { group => rows(group.rows) }
            }
        } else if( isEmptyResultAllowed ) {
            Iterator.empty
        } else {
            val partnNullVals: List[ScalColValue] =
                partnCols.map { col => SqlNull(col.sqlType.baseType) }
            val inputNullRow: ScalTableRow = ScalTableRow(
                input.columns.map { col =>
                    col.name -> SqlNull(col.sqlType.baseType)
                }
            )
            resultRows(partnNullVals, aggregates, inputNullRow).iterator
        }
    }

    private def rows(
        inpRows: Iterator[ScalTableRow]
    ): Iterator[ScalTableRow] = {
        val partnMap: mutable.Map[
            List[ScalColValue], (SeqAggregateRows, ScalTableRow)
        ] = mutable.Map()

        inpRows.foreach { row =>
            val partnVals: List[ScalColValue] =
                partnCols.map { col => row.getScalExpr(col) }

            val partnAggregate: SeqAggregateRows =
                partnMap.get(partnVals) match {
                    case Some((prev, _)) => prev
                    case None => aggregates
                }

            val updPartnAggregate: SeqAggregateRows =
                rowLabels(row).foldLeft (partnAggregate) {
                    case (prev, rowLabel) =>
                        prev.update(evaluator, row, rowLabel)
                }

            partnMap += partnVals -> (updPartnAggregate, row)
        }

        partnMap.iterator.flatMap { case (partnVals, (resAggr, lastRow)) =>
            resultRows(partnVals, resAggr, lastRow)
        }
    }

    private def resultRows(
        partnVals: List[ScalColValue],
        a: SeqAggregateRows,
        lastRow: ScalTableRow
    ): List[ScalTableRow] = a.result(lastRow).map { vals =>
        val aggrColVals: List[(String, ScalColValue)] =
            aggregates.columns.map { col => col.name } zip vals

        val resColVals: List[(String, ScalColValue)] =
            if( isPartnColsInResult ) {
                val partnColVals: List[(String, ScalColValue)] =
                    partnCols.map { col => col.name } zip partnVals

                partnColVals:::aggrColVals
            } else aggrColVals

        ScalTableRow(resColVals)
    }

    override def close(): Unit = { /* empty */ }
}
