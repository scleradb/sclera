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

package com.scleradb.visual.exec

import scala.language.postfixOps

import com.scleradb.sql.expr.{ScalExpr, SortExpr}
import com.scleradb.sql.datatypes.Column

import com.scleradb.visual.model.spec.AxisSetTask
import com.scleradb.visual.model.plot._

object PlotPlanner {
    def plan(
        columns: List[Column],
        resultOrder: List[SortExpr],
        dataPlot: DataPlot,
        isAligned: Boolean
    ): DataPlotInfo = {
        val xAxesSpecs: List[(ScalExpr, List[AxisSetTask])] =
            dataPlot.axesSpecs(AxisX)
        val yAxesSpecs: List[(ScalExpr, List[AxisSetTask])] =
            dataPlot.axesSpecs(AxisY)

        val xAxes: List[Axis] =
            axes(columns, resultOrder, AxisX, xAxesSpecs)
        val yAxes: List[Axis] =
            axes(columns, resultOrder, AxisY, yAxesSpecs)

        val xAxesExprs: List[ScalExpr] = xAxesSpecs.map { case (e, _) => e }
        val yAxesExprs: List[ScalExpr] = yAxesSpecs.map { case (e, _) => e }

        val subPlotInfo: List[((Int, Int), List[Layer])] =
            dataPlot.subPlots.map { p =>
                val xIndex: Int = xAxesExprs indexOf p.xAxisExpr
                val yIndex: Int = yAxesExprs indexOf p.yAxisExpr

                ((xIndex, yIndex), p.layers)
            }
        
        if( isAligned ) planAligned(xAxes, yAxes, subPlotInfo)
        else planWeightedBasic(xAxes, yAxes, subPlotInfo)
    }

    private def planWeightedBasic(
        xAxes: List[Axis],
        yAxes: List[Axis],
        subPlotInfo: List[((Int, Int), List[Layer])]
    ): DataPlotInfo = {
        val outer: Double = 0.0
        val inner: Double = 0.1

        val yAxesWeights: List[Double] = axisWeights(yAxes)
        val ys: List[Int] = subPlotInfo.map { case ((_, yi), _) => yi }
        val yReq: List[Double] = ys.map { yi => yAxesWeights(yi) }
        val yInfo: List[(Double, Double)] = axesLayout(yReq, outer, inner)

        val layout: List[(DataSubPlotDisplay, ((Int, Int), List[Layer]))] =
            subPlotInfo.zip(yInfo).map { case (plotInfo, (y, ylen)) =>
                val subPlotDisplay: DataSubPlotDisplay = DataSubPlotDisplay(
                    x = outer,
                    y = y,
                    width = (1.0 - 2 * outer),
                    height = ylen
                )

                (subPlotDisplay, plotInfo)
            }

        val yLengthMap: Map[Int, Double] = Map() ++
            ys.zip(yInfo.map { case (_, len) => len })

        DataPlotInfo(
            xAxes.map { axis => (axis, (1.0 - 2 * outer)) },
            yAxes.zipWithIndex.map {
                case (yAxis, yi) => (yAxis, yLengthMap(yi))
            },
            layout
        )
    }

    private def planAligned(
        xAxes: List[Axis],
        yAxes: List[Axis],
        subPlotInfo: List[((Int, Int), List[Layer])]
    ): DataPlotInfo = {
        val xs: List[Int] = subPlotInfo.map { case ((xi, _), _) => xi } distinct

        val outer: Double = 0.0
        val inner: Double = 0.1

        val xAxesWeights: List[Double] = axisWeights(xAxes)
        val xInfo: List[(Double, Double)] =
            axesLayout(xAxesWeights, outer, inner)
        val xMap: Map[Int, (Double, Double)] = Map() ++
            xInfo.zipWithIndex.map { case (axisInfo, i) => i -> axisInfo }

        val yAxesWeights: List[Double] = axisWeights(yAxes)
        val yInfo: List[(Double, Double)] =
            axesLayout(yAxesWeights, outer, inner)
        val yMap: Map[Int, (Double, Double)] = Map() ++
            yInfo.zipWithIndex.map { case (axisInfo, i) => i -> axisInfo }

        val uniq: List[((Int, Int), List[Layer])] =
            subPlotInfo.groupBy(_._1).view.mapValues { es =>
                es.flatMap { case (_, ls) => ls }
            } toList

        val layout: List[(DataSubPlotDisplay, ((Int, Int), List[Layer]))] =
            uniq.map { case plotInfo@((xi, yi), _) =>
                val (x, xlen) = xMap(xi)
                val (y, ylen) = yMap(yi)

                val subPlotDisplay: DataSubPlotDisplay = DataSubPlotDisplay(
                    x = x,
                    y = y,
                    width = xlen,
                    height = ylen
                )

                (subPlotDisplay, plotInfo)
            }

        DataPlotInfo(
            xAxes zip xInfo.map { case (_, len) => len },
            yAxes zip yInfo.map { case (_, len) => len },
            layout
        )
    }

    private def axesLayout(
        req: List[Double],
        outer: Double,
        inner: Double
    ): List[(Double, Double)] = {
        val reqSum: Double = req.sum

        val availLen: Double = (1.0 - 2 * outer - (req.length-1) * inner)
        val axesLengths: List[Double] = req.map { w => availLen * w / reqSum }

        axesLengths.scanLeft (outer) { case (taken, nextLength) =>
            taken + nextLength + inner
        } zip axesLengths
    }

    private def axes(
        columns: List[Column],
        resultOrder: List[SortExpr],
        axisId: AxisId,
        axesSpecs: List[(ScalExpr, List[AxisSetTask])]
    ): List[Axis] = axesSpecs.map { case (e, tasks) =>
        val inferredTasks: List[AxisSetTask] =
            PlotInference.axisSetTasks(e, columns, resultOrder)

        PlotProcessor.applyTasks(Axis.default(axisId), inferredTasks ::: tasks)
    }

    private def axisWeights(axes: List[Axis]): List[Double] =
        axes.map { axis => axis.weightOpt getOrElse 1.0 }
}
