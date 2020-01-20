/**
* Sclera - Visualization
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

package com.scleradb.visual.model.plot

import scala.language.postfixOps
import scala.util.Random

import com.scleradb.util.tools.Counter

import com.scleradb.sql.types.{SqlType, SqlFloat, SqlInteger}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.expr.{RelExpr, RelOpExpr, SortExpr}
import com.scleradb.sql.expr.{ScalExpr, SqlNull, ScalOpExpr, Plus}
import com.scleradb.sql.expr.{ScalValueBase, ScalColValue, ColRef}
import com.scleradb.sql.expr.{IntConst, DoubleConst}

import com.scleradb.analytics.transform.objects.Transformer
import com.scleradb.analytics.transform.expr.Transform

/** Position */
sealed abstract class Position extends Transformer {
    val key: List[String] // among the geom values input must be partnd on this
    val inValues: List[String]
    val outValues: List[String]
    val order: List[SortExpr] // must be sorted on this within each key

    val isOffsetRelative: Map[String, Boolean]

    def scalExprs: List[ScalExpr] = order.map { se => se.expr }

    def rewrite(dataExpr: RelExpr, geom: Geom): (RelExpr, PositionOffset) = {
        val in: List[(String, ScalExpr)] =
            inValues.map { name => name -> geom.propMap(name) }
        val keyExprs: List[ScalExpr] = key.map { name => geom.propMap(name) }
        val out: List[(String, ColRef)] =
            outValues.map { name => name -> ColRef(Counter.nextSymbol("PS")) }

        val outDataExpr: RelExpr = Transform.buildExpr(
            Transform.Join, this, in, keyExprs, order, out, dataExpr
        )

        val outCols: List[(String, ColRef)] =
            out.map { case (name, col) => name -> ColRef(col.name) }
        val outOffset: PositionOffset =
            Position.offset(outCols, isOffsetRelative)

        (outDataExpr, outOffset)
    }
}

object Position {
    def dodge(
        key: List[String],
        order: List[SortExpr],
        params: Map[String, Double] = Map()
    ): PositionDodge = {
        val outerPadding: Double = params.get("OUTERPADDING") getOrElse 0.2f
        PositionDodge(key, order, outerPadding)
    }

    def dodgeBar(
        key: List[String],
        order: List[SortExpr],
        params: Map[String, Double] = Map()
    ): PositionDodgeBar = {
        val padding: Double = params.get("PADDING") getOrElse 0.0f
        val outerPadding: Double = params.get("OUTERPADDING") getOrElse 0.2f
        PositionDodgeBar(key, order, padding, outerPadding)
    }

    def stack(
        key: List[String],
        stackables: List[String],
        order: List[SortExpr],
        params: Map[String, Double] = Map()
    ): PositionStack = PositionStack(key, stackables, order)

    def stackBar(
        key: List[String],
        stackables: List[String],
        order: List[SortExpr],
        params: Map[String, Double] = Map()
    ): PositionStackBar = {
        val padding: Double = params.get("PADDING") getOrElse 0.1f
        PositionStackBar(key, stackables, order, padding)
    }

    def jitter(
        key: List[String],
        params: Map[String, Double] = Map()
    ): PositionJitter = PositionJitter(key, params)

    def offset(
        props: List[(String, ColRef)],
        isOffsetRelative: Map[String, Boolean]
    ): PositionOffset = PositionOffset(props, isOffsetRelative)
}

case class PositionDodge(
    override val key: List[String],
    override val order: List[SortExpr],
    outerPadding: Double
) extends Position {
    override val inValues: List[String] = key
    override val outValues: List[String] = key
    override val isOffsetRelative: Map[String, Boolean] = Map() ++
        key.map { v => v -> true }

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] =
        Map() ++ key.map { name => name -> SqlFloat(None) }

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] = {
        val inputList: List[Map[String, ScalColValue]] = input.toList
        val step: Double = 1 / (inputList.size + 2 * outerPadding)

        inputList.iterator.zipWithIndex.map { case (_, i) =>
            Map() ++ key.map { name =>
                name -> DoubleConst(step * (i + outerPadding + 0.5))
            }
        }
    }
}

case class PositionDodgeBar(
    override val key: List[String],
    override val order: List[SortExpr],
    padding: Double,
    outerPadding: Double
) extends Position {
    private val barWidthStr: String = "barwidth"

    override val inValues: List[String] = key
    override val outValues: List[String] = barWidthStr :: key
    override val isOffsetRelative: Map[String, Boolean] = Map() ++
        outValues.map { v => v -> true }

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] =
        Map(barWidthStr -> SqlFloat(None)) ++
        key.map { name => name -> SqlFloat(None) }

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] = {
        val inputList: List[Map[String, ScalColValue]] = input.toList
        val step: Double = 1 / (inputList.size + 2 * outerPadding - padding)
        val barWidth: DoubleConst = DoubleConst(step * (1 - padding))

        inputList.iterator.zipWithIndex.map { case (_, i) =>
            Map(barWidthStr -> barWidth) ++ key.map { name =>
                name -> DoubleConst(step * (i + outerPadding))
            }
        }
    }
}

case class PositionStack(
    override val key: List[String],
    stackables: List[String],
    override val order: List[SortExpr]
) extends Position {
    private val stackIndexStr: String = "stackindex"

    override val inValues: List[String] = key ::: stackables
    override val outValues: List[String] = stackIndexStr :: stackables
    override val isOffsetRelative: Map[String, Boolean] = Map() ++
        outValues.map { v => v -> false }

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] = Map(stackIndexStr -> SqlInteger) ++
        stackables.map { name => name -> SqlFloat(None) }

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] = {
        val init: Map[String, ScalExpr] = Map(stackIndexStr -> IntConst(0)) ++
            outValues.map { name => name -> DoubleConst(0) }

        val head: Map[String, ScalColValue] = input.next()

        input.zipWithIndex.scanLeft ((init, head)) { case ((p, r), (row, i)) =>
            val next: Map[String, ScalExpr] =
                Map(stackIndexStr -> IntConst(i+1)) ++
                stackables.map { name =>
                    name -> ScalOpExpr(Plus, List(p(name), r(name)))
                }

            (next, row)
        } map { case (outMap, _) => outMap }
    }
}

case class PositionStackBar(
    override val key: List[String],
    stackables: List[String],
    override val order: List[SortExpr],
    padding: Double
) extends Position {
    private val barWidthStr: String = "barwidth"

    override val inValues: List[String] = key ::: stackables
    override val outValues: List[String] = barWidthStr :: inValues
    override val isOffsetRelative: Map[String, Boolean] =
        Map(barWidthStr -> true) ++
        key.map { v => v -> true } ++
        stackables.map { v => v -> false }

    private val barWidth: DoubleConst = DoubleConst(1 - padding)

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] =
        Map(barWidthStr -> SqlFloat(None)) ++
        key.map { name => name -> SqlFloat(None) } ++
        stackables.map { name => name -> SqlFloat(None) }

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] = {
        val init: Map[String, ScalExpr] = Map() ++
            outValues.map { name => name -> DoubleConst(0) }

        val head: Map[String, ScalColValue] = input.next()

        input.scanLeft ((init, head)) { case ((prev, r), row) =>
            val next: Map[String, ScalExpr] = Map() ++
                stackables.map { name =>
                    name -> ScalOpExpr(Plus, List(prev(name), r(name)))
                }

            (next, row)
        } map { case (outMap, _) =>
            outMap + (barWidthStr -> barWidth) ++
            key.map { name => name -> DoubleConst(padding/2.0) }
        }
    }
}

case class PositionJitter(
    override val key: List[String],
    params: Map[String, Double]
) extends Position {
    override val order: List[SortExpr] = Nil
    override val inValues: List[String] = key
    override val outValues: List[String] = key
    override val isOffsetRelative: Map[String, Boolean] = Map() ++
        outValues.map { v => v -> false }

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] = Map() ++
        key.map { name => name -> SqlFloat(None) }

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] = input.map { _ => Map() ++
        key.map { name =>
            val width: Double = params.get(name) getOrElse 0.4f
            name -> DoubleConst((2*Random.nextDouble()-1) * width)
        }
    }
}

case class PositionOffset(
    props: List[(String, ColRef)],
    override val isOffsetRelative: Map[String, Boolean]
) extends Position {
    override val key: List[String] = Nil
    override val order: List[SortExpr] = Nil
    override val inValues: List[String] = Nil
    override val outValues: List[String] = Nil

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] =
        inputColTypes

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] =
        input
}
