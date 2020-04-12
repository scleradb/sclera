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

import com.scleradb.sql.expr.{ScalExpr, ColRef, ScalColValue, ScalValueBase}
import com.scleradb.sql.expr.{SqlNull, SortExpr, SortAsc, NullsLast}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow, ExtendedTableRow}
import com.scleradb.sql.result.TableRowGroupIterator

/** Creates disjoint intervals from possibly overlapping intervals
  * Input intervals need to be sorted on (partition cols, interval start (asc))
  * Also, all intervals must have interval start < interval end
  * @param evaluator Scalar expression evaluator
  * @param lhsColRef Column containing the left interval boundary
  * @param rhsColRef Column containing the right interval boundary
  * @param outLhsColRef Column containing the output left interval boundary
  * @param outRhsColRef Column containing the output right interval boundary
  * @param partnCols Partition columns
  * @param input Input stream
  */
class DisjointIntervalTableResult(
    evaluator: ScalExprEvaluator,
    lhsColRef: ColRef,
    rhsColRef: ColRef,
    outLhsColRef: ColRef,
    outRhsColRef: ColRef,
    partnColRefs: List[ColRef],
    input: TableResult
) extends TableResult {
    override val columns: List[Column] = {
        val lhsCol: Column = input.column(lhsColRef)
        val outLhsCol: Column = Column(outLhsColRef.name, lhsCol.sqlType)

        val rhsCol: Column = input.column(rhsColRef)
        val outRhsCol: Column = Column(outRhsColRef.name, rhsCol.sqlType)

        outLhsCol::outRhsCol::input.columns
    }

    private val (partnOrder, partnRowsOrder) =
        input.resultOrder.span { case SortExpr(expr, _, _) =>
            partnColRefs contains expr
        }

    override val resultOrder: List[SortExpr] = partnOrder ::: List(
        SortExpr(outLhsColRef, SortAsc, NullsLast),
        SortExpr(outRhsColRef, SortAsc, NullsLast)
    )

    override def rows: Iterator[ScalTableRow] = {
        val partnExprs: List[ScalExpr] =
            partnOrder.map { case SortExpr(expr, _, _) => expr }

        if( partnExprs.distinct.size != partnColRefs.distinct.size ) {
            throw new RuntimeException(
                "Input needs to be sorted on partition columns"
            )
        }

        partnRowsOrder.headOption match {
            case Some(SortExpr(col: ColRef, SortAsc, _))
                if( col == lhsColRef ) => ()
            case _ => 
                throw new RuntimeException(
                    "Input intervals need to be sorted on interval start"
                )
        }

        if( partnOrder.isEmpty ) rows(input.typedRows) else {
            TableRowGroupIterator(
                evaluator, input.typedRows, partnColRefs
            ).flatMap { group => rows(group.rows) }
        }
    }

    private def rows(
        partnRows: Iterator[ScalTableRow]
    ): Iterator[ScalTableRow] = new Iterator[ScalTableRow] {
        private var outRows: Iterator[ScalTableRow] = Iterator()
        private var buffer: Vector[HistElem] = Vector()

        override def hasNext: Boolean =
            if( outRows.hasNext ) {
                true
            } else if( partnRows.hasNext ) {
                val row: ScalTableRow = partnRows.next()

                val (updRows, updBuffer) = processInputRow(row, buffer)
                outRows = updRows
                buffer = updBuffer

                hasNext
            } else if( buffer.isEmpty ) {
                false
            } else {
                outRows = buffer.head.tableRows
                buffer = buffer.tail

                hasNext
            }

        override def next(): ScalTableRow =
            if( hasNext ) outRows.next() else Iterator.empty.next()
    }

    private def processInputRow(
        row: ScalTableRow,
        buffer: Vector[HistElem]
    ): (Iterator[ScalTableRow], Vector[HistElem]) = {
        val lhs: ScalColValue = row.getScalExpr(lhsColRef)
        val rhs: ScalColValue = row.getScalExpr(rhsColRef)

        if( compare(lhs, rhs) >= 0 ) {
            throw new IllegalArgumentException(
                "Invalid interval: (" + lhs.repr + ", " + rhs.repr + ")"
            )
        }

        val refinedBuffer: Vector[HistElem] = buffer.flatMap { case e =>
            if( compare(e.rhs, lhs) <= 0 || compare(e.lhs, rhs) >= 0 ) {
                // lhs after e / rhs before e
                Vector(e)
            } else if( compare(e.lhs, lhs) < 0 ) { // lhs within e
                if( compare(rhs, e.rhs) < 0 ) { // rhs within e
                    Vector(
                        HistElem(e.lhs, lhs, e.overlapRows),
                        HistElem(lhs, rhs, e.overlapRows :+ row),
                        HistElem(rhs, e.rhs, e.overlapRows)
                    )
                } else { // rhs after e / same as e.rhs
                    Vector(
                        HistElem(e.lhs, lhs, e.overlapRows),
                        HistElem(lhs, e.rhs, e.overlapRows :+ row)
                    )
                }
            } else { // lhs before e / same as e.lhs
                if( compare(rhs, e.rhs) < 0 ) { // rhs within e
                    Vector(
                        HistElem(e.lhs, rhs, e.overlapRows :+ row),
                        HistElem(rhs, e.rhs, e.overlapRows)
                    )
                } else { // rhs after e / same as e.rhs
                    Vector(HistElem(e.lhs, e.rhs, e.overlapRows :+ row))
                }
            }
        }

        val (out, rem) = refinedBuffer.span { e => compare(e.rhs, lhs) <= 0 }

        val updBuffer = rem.lastOption match {
            case None =>
                rem :+ HistElem(lhs, rhs, Vector(row))
            case Some(e)
            if( compare(e.rhs, rhs) < 0 ) =>
                rem :+ HistElem(e.rhs, rhs, Vector(row))
            case _ =>
                rem
        }

        val outRows: Iterator[ScalTableRow] =
            out.iterator.flatMap { e => e.tableRows }

        (outRows, updBuffer)
    }

    private case class HistElem(
        val lhs: ScalColValue,
        val rhs: ScalColValue,
        val overlapRows: Vector[ScalTableRow]
    ) {
        assert(compare(lhs, rhs) < 0)

        private val histMap: Map[String, ScalColValue] = Map(
            outLhsColRef.name -> lhs,
            outRhsColRef.name -> rhs
        )

        def tableRows: Iterator[ScalTableRow] =
            overlapRows.iterator.map { r => ExtendedTableRow(r, histMap) }

        override def toString: String =
            "(" + lhs.repr + ", " + rhs.repr + "): [" +
            overlapRows.map(r => intervalStr(r)).mkString(", ") + "]"

        private def intervalStr(row: ScalTableRow): String = {
            val lhs: ScalColValue = row.getScalExpr(lhsColRef)
            val rhs: ScalColValue = row.getScalExpr(rhsColRef)

            ("(" + lhs.repr + ", " + rhs.repr + ")")
        }
    }

    def compare(x: ScalColValue, y: ScalColValue): Int = (x, y) match {
        case (a: ScalValueBase, b: ScalValueBase) => a compare b
        case (_: SqlNull, _: SqlNull) => 0
        case (_: SqlNull, _) => 1
        case (_, _: SqlNull) => -1
    }

    override def close(): Unit = { }
}

/** Companion object containing the constructor */
object DisjointIntervalTableResult {
    def apply(
        evaluator: ScalExprEvaluator,
        lhsColRef: ColRef,
        rhsColRef: ColRef,
        outLhsColRef: ColRef,
        outRhsColRef: ColRef,
        partnColRefs: List[ColRef],
        input: TableResult
    ): DisjointIntervalTableResult =
        new DisjointIntervalTableResult(
            evaluator,
            lhsColRef, rhsColRef,
            outLhsColRef, outRhsColRef,
            partnColRefs, input
        )
}
