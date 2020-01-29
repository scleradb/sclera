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

import com.scleradb.sql.expr.{RelExpr, ScalExpr, ScalValueBase}

import com.scleradb.visual.model.spec._
import com.scleradb.visual.model.plot._

object PlotProcessor {
    def process(dataExpr: RelExpr, spec: PlotSpec): Plot = {
        val layout: Layout = applyTasks(Layout.default, spec.layoutTasks)
        val transition: Transition =
            applyTasks(Transition.default, spec.transitionTasks)
        val dataPlot: DataPlot =
            applyTasks(DataPlot.default, spec.dataPlotTasks)

        val (updDataExpr, updDataPlot) =
            PlotNormalizer.normalize(dataExpr, dataPlot)

        Plot(updDataExpr, layout, transition, updDataPlot, spec.isAligned)
    }

    private def applyTasks(layout: Layout, tasks: List[LayoutSetTask]): Layout =
        tasks.foldLeft (layout) { case (prevLayout, task) =>
            applyTask(prevLayout, task)
        }

    private def applyTask(
        layout: Layout,
        task: LayoutSetTask
    ): Layout = task match {
        case LayoutSetDisplay(displayTasks) =>
            val display: Display = applyTasks(layout.display, displayTasks)
            Layout(display, layout.coord)

        case LayoutSetCoordinates(coordSetTask) =>
            val coord: Coordinates = applyTask(layout.coord, coordSetTask)
            Layout(layout.display, coord)
    }

    private def applyTasks(
        display: Display,
        tasks: List[DisplaySetTask]
    ): Display =
        tasks.foldLeft (display) { case (prevDisplay, task) =>
            applyTask(prevDisplay, task)
        }

    private def applyTask(
        disp: Display,
        task: DisplaySetTask
    ): Display = task match {
        case DisplaySetWidth(width) =>
            Display(width, disp.height, disp.margin, disp.legendDisplay)
        case DisplaySetHeight(height) =>
            Display(disp.width, height, disp.margin, disp.legendDisplay)
        case DisplaySetMargin(marginTasks) =>
            val margin: DisplayMargin = applyTasks(disp.margin, marginTasks)
            Display(disp.width, disp.height, margin, disp.legendDisplay)
        case DisplaySetLegend(legendDisplayTasks) =>
            val legendDisplay: LegendDisplay =
                applyTasks(disp.legendDisplay, legendDisplayTasks)
            Display(disp.width, disp.height, disp.margin, legendDisplay)
    }

    private def applyTasks(
        margin: DisplayMargin,
        tasks: List[DisplayMarginSetTask]
    ): DisplayMargin =
        tasks.foldLeft (margin) { case (prevDisplayMargin, task) =>
            applyTask(prevDisplayMargin, task)
        }

    private def applyTask(
        margin: DisplayMargin,
        task: DisplayMarginSetTask
    ): DisplayMargin = task match {
        case DisplayMarginSetTop(top) =>
            DisplayMargin(top, margin.right, margin.bottom, margin.left)
        case DisplayMarginSetRight(right) =>
            DisplayMargin(margin.top, right, margin.bottom, margin.left)
        case DisplayMarginSetBottom(bottom) =>
            DisplayMargin(margin.top, margin.right, bottom, margin.left)
        case DisplayMarginSetLeft(left) =>
            DisplayMargin(margin.top, margin.right, margin.bottom, left)
    }

    private def applyTasks(
        legendDisplay: LegendDisplay,
        tasks: List[LegendDisplaySetTask]
    ): LegendDisplay =
        tasks.foldLeft (legendDisplay) { case (prevLegendDisplay, task) =>
            applyTask(prevLegendDisplay, task)
        }

    private def applyTask(
        legendDisplay: LegendDisplay,
        task: LegendDisplaySetTask
    ): LegendDisplay = task match {
        case LegendDisplaySetPadding(padding) =>
            LegendDisplay(padding, legendDisplay.width)
        case LegendDisplaySetWidth(width) =>
            LegendDisplay(legendDisplay.padding, width)
    }

    private def applyTask(
        coord: Coordinates,
        task: CoordSetTask
    ): Coordinates = task match {
        case CoordSetGrid(gridSetTasks) =>
            val grid: CoordGrid = coord match {
                case (coordGrid: CoordGrid) => coordGrid
                case _ => Coordinates.grid
            }

            applyTasks(grid, gridSetTasks)

        case CoordSetMap(mapSetTasks) =>
            val map: CoordMap = coord match {
                case (coordMap: CoordMap) => coordMap
                case _ => Coordinates.map
            }

            applyTasks(map, mapSetTasks)
    }

    private def applyTasks(
        grid: CoordGrid,
        tasks: List[GridSetTask]
    ): CoordGrid =
        tasks.foldLeft (grid) { case (prevGrid, task) =>
            applyTask(prevGrid, task)
        }

    private def applyTask(
        grid: CoordGrid,
        task: GridSetTask
    ): CoordGrid = task match {
        case GridSetType(gridTypeStr) =>
            val gridType: GridType = GridType(gridTypeStr)
            Coordinates.grid(gridType, grid.aes)

        case GridSetAes(tasks) =>
            val aes: GridAes = applyTasks(GridAes.default, tasks)
            Coordinates.grid(grid.gridType, aes)
    }

    private def applyTasks(
        gridAes: GridAes,
        tasks: List[GridAesSetTask]
    ): GridAes =
        tasks.foldLeft (gridAes) { case (prevGridAes, task) =>
            applyTask(prevGridAes, task)
        }

    private def applyTask(
        aes: GridAes,
        task: GridAesSetTask
    ): GridAes = task match {
        case GridAesSetColor(color) =>
            GridAes(color, aes.xAxisAes, aes.yAxisAes)

        case GridAesSetAxis(AxisX, tasks) =>
            val xAxisAes: AxisAes = applyTasks(aes.xAxisAes, tasks)
            GridAes(aes.color, xAxisAes, aes.yAxisAes)

        case GridAesSetAxis(AxisY, tasks) =>
            val yAxisAes: AxisAes = applyTasks(aes.yAxisAes, tasks)
            GridAes(aes.color, aes.xAxisAes, yAxisAes)
    }

    private def applyTasks(
        axisAes: AxisAes,
        tasks: List[AxisAesSetTask]
    ): AxisAes =
        tasks.foldLeft (axisAes) { case (prevAxisAes, task) =>
            applyTask(prevAxisAes, task)
        }

    private def applyTask(
        aes: AxisAes,
        task: AxisAesSetTask
    ): AxisAes = task match {
        case AxisAesSetColor(color) => AxisAes(color, aes.ticks)
        case AxisAesSetTicks(ticks) => AxisAes(aes.color, ticks)
    }

    private def applyTasks(
        map: CoordMap,
        tasks: List[MapSetTask]
    ): CoordMap =
        tasks.foldLeft (map) { case (prevMap, task) =>
            applyTask(prevMap, task)
        }

    private def applyTask(
        map: CoordMap,
        task: MapSetTask
    ): CoordMap = task match {
        case MapSetProjection(projName, paramsOpt) =>
            Coordinates.map(MapProject(projName, paramsOpt), map.orient)

        case MapSetOrientation(a, b, c) =>
            Coordinates.map(map.proj, MapOrient(a, b, c))
    }

    def applyTasks(axis: Axis, tasks: List[AxisSetTask]): Axis =
        tasks.foldLeft (axis) { case (prevAxis, task) =>
            applyTask(prevAxis, task)
        }

    private def applyTask(axis: Axis, task: AxisSetTask): Axis = task match {
        case AxisSetLabel(label) => Axis(
            Some(label), axis.orient, axis.scale, axis.isFree,
            axis.isIncreasing, axis.isZoomIn,
            axis.windowSizeOpt, axis.tickFormatOpt,
            axis.ticksOpt, axis.weightOpt
        )

        case AxisSetScale(scale) => Axis(
            axis.labelOpt, axis.orient, scale, axis.isFree,
            axis.isIncreasing, axis.isZoomIn,
            axis.windowSizeOpt, axis.tickFormatOpt,
            axis.ticksOpt, axis.weightOpt
        )

        case AxisSetFree(isFree) => Axis(
            axis.labelOpt, axis.orient, axis.scale, isFree,
            axis.isIncreasing, axis.isZoomIn,
            axis.windowSizeOpt, axis.tickFormatOpt,
            axis.ticksOpt, axis.weightOpt
        )

        case AxisSetIncreasing(isIncreasing) => Axis(
            axis.labelOpt, axis.orient, axis.scale, axis.isFree,
            isIncreasing, axis.isZoomIn,
            axis.windowSizeOpt, axis.tickFormatOpt,
            axis.ticksOpt, axis.weightOpt
        )

        case AxisSetZoom(isZoomIn) => Axis(
            axis.labelOpt, axis.orient, axis.scale, axis.isFree,
            axis.isIncreasing, isZoomIn,
            axis.windowSizeOpt, axis.tickFormatOpt,
            axis.ticksOpt, axis.weightOpt
        )

        case AxisSetWindowSize(windowSize) => Axis(
            axis.labelOpt, axis.orient, axis.scale, axis.isFree,
            axis.isIncreasing, axis.isZoomIn,
            Some(windowSize), axis.tickFormatOpt,
            axis.ticksOpt, axis.weightOpt
        )

        case AxisSetTickFormat(tickFormat) => Axis(
            axis.labelOpt, axis.orient, axis.scale, axis.isFree,
            axis.isIncreasing, axis.isZoomIn,
            axis.windowSizeOpt, Some(tickFormat),
            axis.ticksOpt, axis.weightOpt
        )

        case AxisSetTicks(ticks) => Axis(
            axis.labelOpt, axis.orient, axis.scale, axis.isFree,
            axis.isIncreasing, axis.isZoomIn,
            axis.windowSizeOpt, axis.tickFormatOpt,
            Some(ticks), axis.weightOpt
        )

        case AxisSetWeight(weight) => Axis(
            axis.labelOpt, axis.orient, axis.scale, axis.isFree,
            axis.isIncreasing, axis.isZoomIn,
            axis.windowSizeOpt, axis.tickFormatOpt,
            axis.ticksOpt, Some(weight)
        )
    }

    private def applyTasks(
        trans: Transition,
        tasks: List[TransitionSetTask]
    ): Transition = 
        tasks.foldLeft (trans) { case (prevTrans, task) =>
            applyTask(prevTrans, task)
        }

    private def applyTask(
        trans: Transition,
        task: TransitionSetTask
    ): Transition = task match {
        case TransitionSetDuration(duration) =>
            Transition(duration, trans.ease)

        case TransitionSetEase(ease) =>
            Transition(trans.duration, ease)
    }

    private def applyTasks(
        dataPlot: DataPlot,
        tasks: List[DataPlotSetTask]
    ): DataPlot =
        tasks.foldLeft (dataPlot) { case (prevDataPlot, task) =>
            applyTask(prevDataPlot, task)
        }

    private def applyTask(
        dataPlot: DataPlot,
        task: DataPlotSetTask
    ): DataPlot = task match {
        case DataPlotSetFacet(facetSetTasks) =>
            val facet: Facet = applyTasks(Facet.default, facetSetTasks)
            DataPlot(Some(facet), dataPlot.subPlots, dataPlot.axisTasks)

        case DataPlotSetAxis(scalExpr, tasks) =>
            val prevTasks: List[AxisSetTask] =
                dataPlot.axisTasks.get(scalExpr) getOrElse Nil
            val axisTasks: Map[ScalExpr, List[AxisSetTask]] =
                dataPlot.axisTasks + (scalExpr -> (prevTasks ::: tasks))

            DataPlot(dataPlot.facetOpt, dataPlot.subPlots, axisTasks)

        case DataPlotSetSubPlot(tasks) =>
            val layers: List[Layer] =
                if( tasks.isEmpty ) List(Layer.default)
                else tasks.map { subPlotSetTask =>
                    applyTasks(Layer.default, subPlotSetTask.tasks)
                }

            val subPlots: List[DataSubPlot] =
                dataPlot.subPlots :+ DataSubPlot(layers)

            DataPlot(dataPlot.facetOpt, subPlots, dataPlot.axisTasks)
    }

    private def applyTasks(
        facet: Facet,
        tasks: List[FacetSetTask]
    ): Facet =
        tasks.foldLeft (facet) { case (prevFacet, task) =>
            applyTask(prevFacet, task)
        }

    private def applyTask(
        facet: Facet,
        task: FacetSetTask
    ): Facet = task match {
        case FacetSetRows(expr) =>
            Facet(Some(expr), facet.colsExprOpt, facet.margins)

        case FacetSetColumns(expr) =>
            Facet(facet.rowsExprOpt, Some(expr), facet.margins)

        case FacetSetMargin(margins) =>
            Facet(facet.rowsExprOpt, facet.colsExprOpt, margins)
    }

    def applyTasks(layer: Layer, tasks: List[LayerSetTask]): Layer =
        tasks.foldLeft (layer) { case (prevLayer, task) =>
            applyTask(prevLayer, task)
        }

    private def applyTask(
        layer: Layer,
        task: LayerSetTask
    ): Layer = task match {
        case LayerSetGeom(geomStr, params) =>
            val geom: Geom = Geom(geomStr, params)
            Layer(
                geom, Aes.default(geom),
                layer.groupOpt, layer.keyOpt,
                layer.posOpt, layer.stats, layer.marks,
                layer.tooltipOpt, layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetAes(prop, tasks) =>
            val aes: Aes = layer.aes

            val p: String = prop.toLowerCase
            val updAesElem: AesElement = applyTasks(p, aes.propMap, tasks)
            val updAes: Aes = Aes(aes.propMap.updated(p, updAesElem))

            Layer(
                layer.geom, updAes,
                layer.groupOpt, layer.keyOpt,
                layer.posOpt, layer.stats, layer.marks,
                layer.tooltipOpt, layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetGroup(group) =>
            Layer(
                layer.geom, layer.aes,
                Some(group), layer.keyOpt,
                layer.posOpt, layer.stats, layer.marks,
                layer.tooltipOpt, layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetKey(key) =>
            Layer(
                layer.geom, layer.aes,
                layer.groupOpt, Some(key),
                layer.posOpt, layer.stats, layer.marks,
                layer.tooltipOpt, layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetPos(posStr, params, order) =>
            Layer(
                layer.geom, layer.aes,
                layer.groupOpt, layer.keyOpt,
                layer.geom.positionOpt(posStr, params, order),
                layer.stats, layer.marks,
                layer.tooltipOpt, layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetStat(statStr, params, tasks) =>
            Layer(
                layer.geom, layer.aes,
                layer.groupOpt, layer.keyOpt,
                layer.posOpt, layer.stats :+ (Stat(statStr, params), tasks),
                layer.marks, layer.tooltipOpt, layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetMark(axisIdOpt, predicate, tasks) =>
            Layer(
                layer.geom, layer.aes,
                layer.groupOpt, layer.keyOpt,
                layer.posOpt, layer.stats,
                layer.marks :+ (Mark(predicate, tasks), axisIdOpt),
                layer.tooltipOpt, layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetTooltip(expr) =>
            Layer(
                layer.geom, layer.aes,
                layer.groupOpt, layer.keyOpt,
                layer.posOpt, layer.stats, layer.marks,
                Some(expr), layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerCopyTooltip(prop) =>
            val expr: ScalExpr = layer.aes.propMap.get(prop.toLowerCase) match {
                case Some(AesConst(v)) => v
                case Some(AesExpr(expr, _, _, _)) => expr
                case None =>
                    throw new IllegalArgumentException(
                        "Invalid aesthetics property: " + prop
                    )
            }

            Layer(
                layer.geom, layer.aes,
                layer.groupOpt, layer.keyOpt,
                layer.posOpt, layer.stats, layer.marks,
                Some(expr), layer.isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetHidden(isHidden) =>
            Layer(
                layer.geom, layer.aes,
                layer.groupOpt, layer.keyOpt,
                layer.posOpt, layer.stats, layer.marks,
                layer.tooltipOpt, isHidden,
                layer.isGenerated, layer.displayOrder
            )

        case LayerSetDisplayOrder(displayOrder) =>
            Layer(
                layer.geom, layer.aes,
                layer.groupOpt, layer.keyOpt,
                layer.posOpt, layer.stats, layer.marks,
                layer.tooltipOpt, layer.isHidden,
                layer.isGenerated, displayOrder
            )
    }

    private def applyTasks(
        prop: String,
        propMap: Map[String, AesElement],
        tasks: List[AesSetTask]
    ): AesElement = {
        val aesElem: AesElement = propMap.get(prop) getOrElse {
            throw new IllegalArgumentException(
                "Invalid aesthetics property: " + prop
            )
        }

        tasks.foldLeft (aesElem) {
            case (prevAesConst: AesConst, task) =>
                applyTask(prevAesConst, prop, propMap, task)
            case (prevAesExpr: AesExpr, task) =>
                applyTask(prevAesExpr, propMap, task)
        }
    }

    private def applyTask(
        aesConst: AesConst,
        prop: String,
        propMap: Map[String, AesElement],
        task: AesSetTask
    ): AesElement = task match {
        case AesSetValue(v: ScalValueBase) =>
            AesConst(v)

        case AesSetValue(expr) =>
            AesExpr(expr, Scale.default(prop), None, None)

        case AesCopyValue(copyProp) =>
            aesCopyValue(copyProp.toLowerCase, propMap, Scale.default(prop))

        case _ => // ignore
            aesConst
    }

    private def applyTask(
        aesExpr: AesExpr,
        propMap: Map[String, AesElement],
        task: AesSetTask
    ): AesElement = task match {
        case AesSetValue(v: ScalValueBase) =>
            AesConst(v)

        case AesSetValue(expr) =>
            AesExpr(expr, aesExpr.scale, aesExpr.onNullOpt, aesExpr.legendOpt)

        case AesCopyValue(copyProp) =>
            aesCopyValue(copyProp.toLowerCase, propMap, aesExpr.scale)

        case AesSetIdentityScale =>
            AesExpr(
                aesExpr.value, Scale.identity(aesExpr.scale),
                aesExpr.onNullOpt, aesExpr.legendOpt
            )

        case AesSetScale(scale) =>
            AesExpr(
                aesExpr.value, scale,
                aesExpr.onNullOpt, aesExpr.legendOpt
            )

        case AesSetOnNull(onNull) =>
            AesExpr(
                aesExpr.value, aesExpr.scale,
                Some(onNull), aesExpr.legendOpt
            )

        case AesSetLegend(tasks: List[LegendSetTask]) =>
            val legend: Legend =
                applyTasks(aesExpr.legendOpt getOrElse Legend.vertical, tasks)
            AesExpr(
                aesExpr.value, aesExpr.scale,
                aesExpr.onNullOpt, Some(legend)
            )
    }

    private def aesCopyValue(
        copyProp: String,
        propMap: Map[String, AesElement],
        propScale: Scale
    ): AesElement = propMap.get(copyProp) match {
        case Some(elem: AesConst) => elem
        case Some(AesExpr(expr, _: IdentityScale, onNullOpt, _)) =>
            AesExpr(expr, Scale.identity(propScale), onNullOpt, None)
        case Some(AesExpr(expr, _, _, _)) =>
            AesExpr(expr, propScale, None, None)
        case None =>
            throw new IllegalArgumentException(
                "Invalid aesthetics property: " + copyProp
            )
    }

    private def applyTasks(legend: Legend, tasks: List[LegendSetTask]): Legend =
        tasks.foldLeft (legend) { case (prevLegend, task) =>
            applyTask(prevLegend, task)
        }

    private def applyTask(
        legend: Legend,
        task: LegendSetTask
    ): Legend = task match {
        case LegendSetOrientation(orient) =>
            Legend(
                LegendOrient(orient), legend.titleOpt,
                legend.labelsOpt, legend.isReversed
            )

        case LegendSetTitle(title) =>
            Legend(
                legend.orient, Some(title),
                legend.labelsOpt, legend.isReversed
            )

        case LegendSetLabels(labels) =>
            Legend(
                legend.orient, legend.titleOpt,
                Some(labels), legend.isReversed
            )

        case LegendSetLabelAlign(alignStr) =>
            legend.orient match {
                case LegendVertical => legend
                case LegendHorizontal(_) =>
                    val align: LabelAlign = LabelAlign(alignStr)
                    Legend(
                        LegendHorizontal(align), legend.titleOpt,
                        legend.labelsOpt, legend.isReversed
                    )
            }

        case LegendSetLabelOrder(isReversed) =>
            Legend(
                legend.orient, legend.titleOpt,
                legend.labelsOpt, isReversed
            )
    }
}
