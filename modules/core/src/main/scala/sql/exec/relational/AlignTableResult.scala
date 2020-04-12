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

import scala.language.postfixOps
import scala.math.Ordering.Float.TotalOrdering
import scala.math.{min, max}

import com.scleradb.sql.expr.{ScalExpr, SortExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow, ConcatTableRow}

/** Computes the equi-join of two input streams
  * @param evaluator Scalar expression evaluator
  * @param distExpr Expression for the distance metric
  * @param marginOpt Margin for matching (max lead/lag)
  * @param inputs Input streams
  */
class AlignTableResult(
    evaluator: ScalExprEvaluator,
    distExpr: ScalExpr,
    marginOpt: Option[Int],
    inputs: List[TableResult]
) extends TableResult {
    sealed abstract class Dist {
        val value: Float
        def trace: List[ScalTableRow]
    }

    case class ConstDist(override val value: Float) extends Dist {
        override def trace: List[ScalTableRow] = Nil
    }

    case class RowDist(row: ScalTableRow, prev: Dist) extends Dist {
        private def dist: Float =
            ScalCastEvaluator.valueAsFloatOpt(
                evaluator.eval(distExpr, row)
            ) getOrElse 0.0f

        override lazy val value: Float = prev.value + dist

        override def trace: List[ScalTableRow] = row::prev.trace
    }

    override val columns: List[Column] = inputs.flatMap { inp => inp.columns }

    override val resultOrder: List[SortExpr] =
        inputs.head.resultOrder match {
            case Nil => inputs.tail.head.resultOrder
            case nonempty => nonempty
        }

    override def rows: Iterator[ScalTableRow] =
        if( distExpr.isConstant || marginOpt == Some(0) ) { // zip the rows
            inputs(0).typedRows.zip(inputs(1).typedRows).map { case (ar, br) =>
                ConcatTableRow(ar, br)
            }
        } else {
            val a: Seq[ScalTableRow] = inputs(0).typedRows.toVector.reverse
            val b: Seq[ScalTableRow] = inputs(1).typedRows.toVector.reverse

            val size: Int = min(a.size, b.size)
            val margin: Int = marginOpt match {
                case Some(m) => max(1, min(m.abs, size-1))
                case None => max(1, size-1)
            }

            val zero: Dist = ConstDist(0.0f)
            val zinf: Dist = ConstDist(Float.PositiveInfinity)
            val z: Seq[Dist] = (0 until margin).map { _ => zinf } toVector

            val (dist, _, _) =
                (0 until size).foldLeft (zero, z, z) { case ((p, ap, bp), k) =>
                    val ak: ScalTableRow = a(k)
                    val bk: ScalTableRow = b(k)

                    val nmin: Dist = List(p, ap(0), bp(0)).minBy { _.value }
                    val n: Dist = RowDist(ConcatTableRow(ak, bk), nmin)

                    val m: Int = min(size - k - 1, margin)

                    val an: Seq[Dist] =
                        (0 until m).scanLeft (n) { case (v, i) =>
                            val amin: Dist =
                                if( i < ap.size-1 )
                                    List(v, ap(i), ap(i+1)).minBy { _.value }
                                else List(v, ap(i)).minBy { _.value }

                            RowDist(ConcatTableRow(a(k+i+1), bk), amin)
                        }

                    val bn: Seq[Dist] =
                        (0 until m).scanLeft (n) { case (v, i) =>
                            val bmin: Dist =
                                if( i < bp.size-1 )
                                    List(v, bp(i), bp(i+1)).minBy { _.value }
                                else List(v, bp(i)).minBy { _.value }

                            RowDist(ConcatTableRow(ak, b(k+i+1)), bmin)
                        }

                    (n, an.toVector.tail, bn.toVector.tail)
                }

            dist.trace.iterator
        }

    override def close(): Unit = { }
}

/** Companion object containing the constructor */
object AlignTableResult {
    def apply(
        evaluator: ScalExprEvaluator,
        distExpr: ScalExpr,
        marginOpt: Option[Int],
        inputs: List[TableResult]
    ): AlignTableResult =
        new AlignTableResult(evaluator, distExpr, marginOpt, inputs)
}
