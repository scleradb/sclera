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

import com.scleradb.sql.expr.ScalValueBase

/** Legend */
case class Legend(
    orient: LegendOrient,
    titleOpt: Option[String],
    labelsOpt: Option[LegendLabels],
    isReversed: Boolean
)

object Legend {
    lazy val vertical: Legend = vertical(None, None)

    def vertical(
        titleOpt: Option[String],
        labelsOpt: Option[LegendLabels]
    ): Legend = Legend(
        LegendOrient.vertical, titleOpt, labelsOpt, isReversed = false
    )

    lazy val horizontal: Legend = horizontal(None, None)

    def horizontal(
        titleOpt: Option[String],
        labelsOpt: Option[LegendLabels]
    ): Legend = Legend(
        LegendOrient.horizontal, titleOpt, labelsOpt, isReversed = false
    )

    def horizontal(
        align: LabelAlign,
        titleOpt: Option[String],
        labelsOpt: Option[LegendLabels]
    ): Legend = Legend(
        LegendOrient.horizontal(align), titleOpt, labelsOpt, isReversed = false
    )
}

/** Legend orientation */
sealed abstract class LegendOrient

object LegendOrient {
    def vertical: LegendOrient = LegendVertical

    lazy val horizontal: LegendOrient = horizontal(LabelAlign.middle)
    def horizontal(align: LabelAlign): LegendOrient = LegendHorizontal(align)

    def apply(s: String): LegendOrient =
        s.toUpperCase match {
            case "VERTICAL" => vertical
            case "HORIZONTAL" => horizontal
            case _ =>
                throw new IllegalArgumentException(
                    "Invalid axis orientation specification: " + s
                )
        }
}

case object LegendVertical extends LegendOrient
case class LegendHorizontal(align: LabelAlign) extends LegendOrient

/** Legend label alignment */
sealed abstract class LabelAlign

object LabelAlign {
    def start: LabelAlign = LabelStart
    def middle: LabelAlign = LabelMiddle
    def end: LabelAlign = LabelEnd

    def apply(s: String): LabelAlign =
        s.toUpperCase match {
            case "START" => start
            case "MIDDLE" => middle
            case "END" => end
            case _ =>
                throw new IllegalArgumentException(
                    "Invalid label alignment specification: " + s
                )
        }
}

case object LabelStart extends LabelAlign
case object LabelMiddle extends LabelAlign
case object LabelEnd extends LabelAlign

/** Legend labels */
sealed abstract class LegendLabels

object LegendLabels {
    def apply(kvs: List[(ScalValueBase, Option[String])]): LegendLabels = {
        val (ks, vs) = kvs.unzip

        if( vs.flatten.isEmpty )
            LegendLabelList(ks.map { k => k.value.toString })
        else {
            val map: List[(ScalValueBase, String)] = kvs.map { case (k, vOpt) =>
                (k, vOpt getOrElse k.value.toString)
            }

            LegendLabelMap(map)
        }
    }
}

case class LegendLabelList(labels: List[String]) extends LegendLabels
case class LegendLabelMap(
    labelMap: List[(ScalValueBase, String)]
) extends LegendLabels
