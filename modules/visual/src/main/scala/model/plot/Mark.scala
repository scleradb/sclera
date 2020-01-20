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

import com.scleradb.util.tools.Counter
import com.scleradb.util.automata.datatypes.Label

import com.scleradb.analytics.sequence.labeler.ColumnRowLabeler
import com.scleradb.analytics.sequence.matcher.expr.Match

import com.scleradb.sql.types.SqlCharVarying
import com.scleradb.sql.expr._

import com.scleradb.visual.model.spec.LayerSetTask
import com.scleradb.visual.exec.PlotProcessor

case class Mark(
    predicate: ScalExpr,
    layerTasks: List[LayerSetTask]
) {
    def scalExprs: List[ScalExpr] =
        predicate :: layerTasks.flatMap { task => task.scalExprs }

    private val label: String = "A"
    private val regexStr: String = label + "*"

    def rewrite(
        plotProc: PlotProcessor,
        dataExpr: RelExpr,
        groupColOpt: Option[ColRef],
        keyCol: ColRef,
        geom: Geom,
        geomTargets: List[ScalarTarget],
        facetCols: List[ColRef]
    ): (RelExpr, Layer) = {
        val predLabelExpr: CaseExpr = CaseExpr(
            BoolConst(true),
            List((predicate, CharConst(label))),
            SqlNull(SqlCharVarying(None))
        )

        val predCol: ColRef = ColRef(Counter.nextSymbol("PRED"))
        val predTargets: List[ScalarTarget] =
            ScalarTarget(predLabelExpr, predCol) ::
            dataExpr.tableColRefs.map { col => RenameCol(col, col) }
        val predDataExpr: RelExpr =
            RelOpExpr(Project(predTargets), List(dataExpr))

        val labeler: ColumnRowLabeler = ColumnRowLabeler(predCol)
        val partnCols: List[ColRef] =
            (facetCols ::: groupColOpt.toList).distinct
        val matchOp: Match = Match(regexStr, Some(labeler), partnCols)

        val matchInputExpr: RelExpr =
            if( partnCols.isEmpty ) predDataExpr else {
                val sorted: List[SortExpr] =
                    predDataExpr.resultOrder.takeWhile {
                        case SortExpr(e, _, _) => partnCols contains e
                    }
                val sortedExprs: List[ScalExpr] =
                    sorted.map { case SortExpr(e, _, _) => e }
                val remSort: List[SortExpr] =
                    partnCols.diff(sortedExprs).map { e => SortExpr(e) }

                RelOpExpr(Order(sorted ::: remSort), List(predDataExpr))
            }

        val matchDataExpr: RelExpr = RelOpExpr(matchOp, List(matchInputExpr))

        val markTargets: List[ScalarTarget] =
            matchDataExpr.tableColRefs.map { col => RenameCol(col, col) } :::
            geomTargets

        val markDataExpr: RelExpr =
            RelOpExpr(Project(markTargets), List(matchDataExpr))

        val markLayer: Layer = plotProc.applyTasks(
            Layer(
                geom, Aes.default(geom),
                groupOpt = groupColOpt, keyOpt = Some(keyCol),
                posOpt = None, stats = Nil, marks = Nil, tooltipOpt = None,
                isHidden = false, isGenerated = true, displayOrder = -1.0
            ),
            layerTasks
        )

        (markDataExpr, markLayer)
    }

    def geomTarget(
        axisId: AxisId,
        col: ColRef
    ): (Geom, ColRef, List[ScalarTarget]) = {
        val keyCol: LabeledColRef =
            LabeledColRef(List(label), Some(0), col.name)
        val keyColAlias: ColRef = ColRef(Counter.nextSymbol("KEY"))

        val minCol: ScalExpr =
            ScalOpExpr(LabeledFunction("MIN", List(label)), List(col))
        val minColAlias: ColRef = ColRef(Counter.nextSymbol("MIN"))

        val maxCol: ScalExpr =
            ScalOpExpr(LabeledFunction("MAX", List(label)), List(col))
        val maxColAlias: ColRef = ColRef(Counter.nextSymbol("MAX"))

        val geom: Region = Geom.region(
            axisId, min = minColAlias, max = maxColAlias
        )

        val targetExprs: List[ScalarTarget] = List(
            ScalarTarget(keyCol, keyColAlias),
            ScalarTarget(minCol, minColAlias),
            ScalarTarget(maxCol, maxColAlias)
        )

        (geom, keyColAlias, targetExprs)
    }

    def geomTarget(
        xcol: ColRef,
        ycol: ColRef
    ): (Geom, ColRef, List[ScalarTarget]) = {
        val keyCol: ScalExpr = ScalOpExpr(
            ScalarFunction("CONCAT"), List(
                ScalOpExpr(
                    TypeCast(SqlCharVarying(None)), List(
                        LabeledColRef(List(label), Some(0), xcol.name)
                    )
                ),
                CharConst("$SCLERA$"),
                ScalOpExpr(
                    TypeCast(SqlCharVarying(None)), List(
                        LabeledColRef(List(label), Some(0), ycol.name)
                    )
                )
            )
        )
        val keyColAlias: ColRef = ColRef(Counter.nextSymbol("KEY"))

        val xminCol: ScalExpr =
            ScalOpExpr(LabeledFunction("MIN", List(label)), List(xcol))
        val xminColAlias: ColRef = ColRef(Counter.nextSymbol("MIN"))

        val xmaxCol: ScalExpr =
            ScalOpExpr(LabeledFunction("MAX", List(label)), List(xcol))
        val xmaxColAlias: ColRef = ColRef(Counter.nextSymbol("MAX"))

        val yminCol: ScalExpr =
            ScalOpExpr(LabeledFunction("MIN", List(label)), List(ycol))
        val yminColAlias: ColRef = ColRef(Counter.nextSymbol("MIN"))

        val ymaxCol: ScalExpr =
            ScalOpExpr(LabeledFunction("MAX", List(label)), List(ycol))
        val ymaxColAlias: ColRef = ColRef(Counter.nextSymbol("MAX"))

        val geom: Rect = Geom.rect(
            xmin = xminColAlias, xmax = xmaxColAlias,
            ymin = yminColAlias, ymax = ymaxColAlias
        )

        val targetExprs: List[ScalarTarget] = List(
            ScalarTarget(keyCol, keyColAlias),
            ScalarTarget(xminCol, xminColAlias),
            ScalarTarget(xmaxCol, xmaxColAlias),
            ScalarTarget(yminCol, yminColAlias),
            ScalarTarget(ymaxCol, ymaxColAlias)
        )

        (geom, keyColAlias, targetExprs)
    }
}
