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

import com.scleradb.util.tools.Counter

import com.scleradb.sql.expr._

import com.scleradb.visual.model.plot._
import com.scleradb.visual.model.spec._

object PlotNormalizer {
    def normalize(
        inpDataExpr: RelExpr,
        inpDataPlot: DataPlot
    ): (RelExpr, DataPlot) = {
        val inpSortScalExprs: List[ScalExpr] =
            inpDataExpr.resultOrder.map { se => se.expr }

        val (dataExpr, map) = dataExprRewrite(
            inpDataExpr, inpSortScalExprs ::: inpDataPlot.scalExprs,
            inpDataPlot.facetExprs
        )

        val dataPlot: DataPlot = normalize(inpDataPlot, map)

        val (updDataExpr, updSubPlots) =
            dataPlot.subPlots.foldLeft ((dataExpr, List[DataSubPlot]())) {
                case ((prevDataExpr, prevSubPlots), DataSubPlot(layers)) =>
                    val (markDataExpr, markLayers) = marksRewrite(
                        prevDataExpr, dataPlot.facetExprs, layers
                    )
                    val (statDataExpr, statLayers) = statsRewrite(
                        markDataExpr, dataPlot.facetExprs, markLayers
                    )
                    val (posDataExpr, posLayers) = positionRewrite(
                        statDataExpr, dataPlot.facetExprs, statLayers
                    )

                    (posDataExpr, prevSubPlots :+ DataSubPlot(posLayers))
            }

        // restore the input order, if any
        val isResultOrderCompatible: Boolean = SortExpr.isSubsumedBy(
            inpDataExpr.resultOrder, updDataExpr.resultOrder
        )

        val finalDataExpr: RelExpr =
            if( isResultOrderCompatible ) updDataExpr
            else RelOpExpr(Order(inpDataExpr.resultOrder), List(updDataExpr))

        val updDataPlot: DataPlot =
            DataPlot(dataPlot.facetOpt, updSubPlots, dataPlot.axisTasks)

        (finalDataExpr, updDataPlot)
    }

    private def normalize(
        dataPlot: DataPlot,
        map: Map[ScalExpr, ColRef]
    ): DataPlot = {
        val facetOpt: Option[Facet] =
            dataPlot.facetOpt.map { facet => normalize(facet, map) }
        val subPlots: List[DataSubPlot] =
            dataPlot.subPlots.map { p => normalize(p, map) }
        val axisTasks: Map[ScalExpr, List[AxisSetTask]] =
            dataPlot.axisTasks.map { case(e, ts) => normalize(e, map) -> ts }

        DataPlot(facetOpt, subPlots, axisTasks)
    }

    private def normalize(
        dataSubPlot: DataSubPlot,
        map: Map[ScalExpr, ColRef]
    ): DataSubPlot = {
        val layers: List[Layer] =
            dataSubPlot.layers.map { layer => normalize(layer, map) }
        DataSubPlot(layers)
    }

    private def normalize(
        layer: Layer,
        map: Map[ScalExpr, ColRef]
    ): Layer = {
        val geom: Geom = normalize(layer.geom, map)
        val aes: Aes = normalize(layer.aes, map)

        val groupOpt: Option[ScalExpr] =
            layer.groupOpt.map { group => normalize(group, map) }
        val keyOpt: Option[ScalExpr] =
            layer.keyOpt.map { key => normalize(key, map) }
        val posOpt: Option[Position] =
            layer.posOpt.map { pos => normalize(pos, map) }
        val stats: List[(Stat, List[LayerSetTask])] =
            layer.stats.map { case (stat, tasks) =>
                (normalize(stat, map), tasks)
            }

        val marks: List[(Mark, Option[AxisId])] =
            layer.marks.map { case (mark, axisIdOpt) =>
                (normalize(mark, map), axisIdOpt)
            }

        val tooltipOpt: Option[ScalExpr] =
            layer.tooltipOpt.map { expr => normalize(expr, map) }

        Layer(
            geom, aes, groupOpt, keyOpt, posOpt, stats, marks, tooltipOpt,
            layer.isHidden, layer.isGenerated, layer.displayOrder
        )
    }

    private def normalize(
        geom: Geom,
        map: Map[ScalExpr, ColRef]
    ): Geom = geom match {
        case blank@Blank => blank

        case Point(x, y) =>
            Point(
                normalize(x, map),
                normalize(y, map)
            )

        case Line(x, y, isClosed, isArea) =>
            Line(
                normalize(x, map),
                normalize(y, map),
                isClosed,
                isArea
            )

        case PointRangeY(x, y, ymin, ymax) =>
            PointRangeY(
                normalize(x, map),
                normalize(y, map),
                normalize(ymin, map),
                normalize(ymax, map)
            )

        case ABLine(yIntercept, slope) =>
            ABLine(
                normalize(yIntercept, map),
                normalize(slope, map)
            )

        case VLine(xIntercept) =>
            VLine(
                normalize(xIntercept, map)
            )

        case Segment(x, xend, y, yend, isArrow) =>
            Segment(
                normalize(x, map),
                normalize(xend, map),
                normalize(y, map),
                normalize(yend, map),
                isArrow
            )

        case Rect(xmin, xmax, ymin, ymax) =>
            Rect(
                normalize(xmin, map),
                normalize(xmax, map),
                normalize(ymin, map),
                normalize(ymax, map)
            )

        case Bar(x, y) =>
            Bar(
                normalize(x, map),
                normalize(y, map)
            )

        case RegionX(min, max, beginOpt, endOpt) =>
            RegionX(
                normalize(min, map),
                normalize(max, map),
                beginOpt.map { begin => normalize(begin, map) },
                endOpt.map { end => normalize(end, map) }
            )

        case RegionY(min, max, beginOpt, endOpt) =>
            RegionY(
                normalize(min, map),
                normalize(max, map),
                beginOpt.map { begin => normalize(begin, map) },
                endOpt.map { end => normalize(end, map) }
            )

        case BoxPlot(lower, middle, upper, x, ymin, ymax, outlierSize) =>
            BoxPlot(
                normalize(lower, map),
                normalize(middle, map),
                normalize(upper, map),
                normalize(x, map),
                normalize(ymin, map),
                normalize(ymax, map),
                normalize(outlierSize, map)
            )

        case RangeY(x, ymin, ymax, widthOpt) =>
            RangeY(
                normalize(x, map),
                normalize(ymin, map),
                normalize(ymax, map),
                widthOpt.map { width => normalize(width, map) }
            )

        case RangeX(xmin, xmax, y, heightOpt) =>
            RangeX(
                normalize(xmin, map),
                normalize(xmax, map),
                normalize(y, map),
                heightOpt.map { height => normalize(height, map) }
            )

        case Ribbon(x, ymin, ymax) =>
            Ribbon(
                normalize(x, map),
                normalize(ymin, map),
                normalize(ymax, map)
            )

        case Ticker(style, widthOpt, ts, open, high, low, close) =>
            Ticker(
                style,
                widthOpt.map { width => normalize(width, map) },
                normalize(ts, map),
                normalize(open, map),
                normalize(high, map),
                normalize(low, map),
                normalize(close, map)
            )

        case GeoMap(mapId) =>
            GeoMap(
                normalize(mapId, map)
            )

        case Text(label, x, y) =>
            Text(
                normalize(label, map),
                normalize(x, map),
                normalize(y, map)
            )
    }

    private def normalize(
        aes: Aes,
        map: Map[ScalExpr, ColRef]
    ): Aes = Aes(
        size = normalize(aes.size, map),
        shape = normalize(aes.shape, map),
        fill = normalize(aes.fill, map),
        alpha = normalize(aes.alpha, map),
        stroke = normalize(aes.stroke, map),
        strokeDashArray = normalize(aes.strokeDashArray, map),
        strokeWidth = normalize(aes.strokeWidth, map),
        interpolate = aes.interpolate,
        tension = aes.tension,
        hjust = normalize(aes.hjust, map),
        vjust = normalize(aes.vjust, map)
    )

    private def normalize(
        aesElem: AesElement,
        map: Map[ScalExpr, ColRef]
    ): AesElement = aesElem match {
        case (v: AesConst) => v
        case (e: AesExpr) =>
            AesExpr(normalize(e.value, map), e.scale, e.onNullOpt, e.legendOpt)
    }

    private def normalize(
        pos: Position,
        map: Map[ScalExpr, ColRef]
    ): Position = pos match {
        case PositionDodge(key, order, outerPadding) =>
            val updOrder: List[SortExpr] = order.map { e => normalize(e, map) }
            PositionDodge(key, updOrder, outerPadding)

        case PositionDodgeBar(key, order, padding, outerPadding) =>
            val updOrder: List[SortExpr] = order.map { e => normalize(e, map) }
            PositionDodgeBar(key, updOrder, padding, outerPadding)

        case PositionStack(key, stackables, order) =>
            val updOrder: List[SortExpr] = order.map { e => normalize(e, map) }
            PositionStack(key, stackables, updOrder)

        case PositionStackBar(key, stackables, order, padding) =>
            val updOrder: List[SortExpr] = order.map { e => normalize(e, map) }
            PositionStackBar(key, stackables, updOrder, padding)

        case p => p
    }

    private def normalize(
        stat: Stat,
        map: Map[ScalExpr, ColRef]
    ): Stat = stat match {
        case (bin: Bin) => bin

        case (bin2d: Bin2d) => bin2d

        case LoessSmooth(bandWidthOpt, itersOpt, accuracyOpt, weightExprOpt) =>
            LoessSmooth(
                bandWidthOpt, itersOpt, accuracyOpt,
                weightExprOpt.map { weightExpr =>  normalize(weightExpr, map) }
            )
    }

    private def normalize(
        mark: Mark,
        map: Map[ScalExpr, ColRef]
    ): Mark = {
        val predicate: ScalExpr = normalize(mark.predicate, map)
        val layerTasks: List[LayerSetTask] =
            mark.layerTasks.map { task => normalize(task, map) }

        Mark(predicate, layerTasks)
    }

    def normalize(
        task: LayerSetTask,
        map: Map[ScalExpr, ColRef]
    ): LayerSetTask = task match {
        case LayerSetGeom(geom, params) =>
            LayerSetGeom(
                geom,
                params.map { case (prop, expr) => prop -> normalize(expr, map) }
            )

        case LayerSetStat(stat, params, tasks) =>
            LayerSetStat(
                stat,
                params.map { case (prop, e) => prop -> normalize(e, map) },
                tasks
            )

        case LayerSetAes(prop, aesSetTasks) =>
            LayerSetAes(prop, aesSetTasks.map { t => normalize(t, map) })

        case LayerSetPos(pos, params, order) =>
            LayerSetPos(pos, params, order.map { se => normalize(se, map) })

        case LayerSetTooltip(expr) =>
            LayerSetTooltip(normalize(expr, map))

        case other => other
    }

    private def normalize(
        task: AesSetTask,
        map: Map[ScalExpr, ColRef]
    ): AesSetTask = task match {
        case AesSetValue(expr) => AesSetValue(normalize(expr, map))
        case other => other
    }

    private def normalize(
        se: SortExpr,
        map: Map[ScalExpr, ColRef]
    ): SortExpr = se match { case SortExpr(e, sortDir, nullsOrder) =>
        SortExpr(normalize(e, map), sortDir, nullsOrder)
    }

    private def normalize(
        e: ScalExpr,
        map: Map[ScalExpr, ColRef]
    ): ScalExpr = map.get(e) getOrElse e

    private def dataExprRewrite(
        dataExpr: RelExpr,
        scalExprs: List[ScalExpr],
        facetExprs: List[ScalExpr]
    ): (RelExpr, Map[ScalExpr, ColRef]) = {
        val targetExprs: List[ScalExpr] =
            scalExprs.filter {
                case (_: ScalColValue) => false // need not be computed
                case _ => true
            } distinct

        val targets: List[ScalarTarget] =
            targetExprs.map {
                case (col: ColRef) => ScalarTarget(col, col)
                case e => ScalarTarget(e, ColRef(Counter.nextSymbol("X")))
            }

        val targetMap: Map[ScalExpr, ColRef] =
            Map() ++ targets.map { t => t.expr -> t.alias }

        val updDataExpr: RelExpr = if( targets.isEmpty ) dataExpr else {
            // need to evaluate before project to preserve sort order
            val input: RelExpr = RelOpExpr(EvaluateOp, List(dataExpr))
            RelOpExpr(Project(targets), List(input))
        }

        (updDataExpr, targetMap)
    }

    private def marksRewrite(
        dataExpr: RelExpr,
        facetExprs: List[ScalExpr],
        layers: List[Layer]
    ): (RelExpr, List[Layer]) = {
        layers.foldLeft (dataExpr, List[Layer]()) {
            case ((prevExpr, prevLayers), layer) =>
                if( layer.marks.isEmpty ) (prevExpr, prevLayers :+ layer) else {
                    val (nextExpr, markLayers) =
                        marksRewrite(prevExpr, facetExprs, layer)

                    val updLayer: Layer = Layer(
                        layer.geom, layer.aes, layer.groupOpt, layer.keyOpt,
                        layer.posOpt, layer.stats, marks = Nil,
                        layer.tooltipOpt, layer.isHidden,
                        layer.isGenerated, layer.displayOrder
                    )

                    val nextLayers: List[Layer] =
                        (prevLayers ::: markLayers) :+ updLayer

                    (nextExpr, nextLayers)
                }
        }
    }

    private def marksRewrite(
        dataExpr: RelExpr,
        facetExprs: List[ScalExpr],
        layer: Layer
    ): (RelExpr, List[Layer]) = {
        def column(expr: ScalExpr): ColRef = expr match {
            case (col: ColRef) => col
            case other => throw new RuntimeException(
                "Expression not normalized: \"" + other.repr + "\""
            )
        }

        def axisColumn(exprOpt: Option[ScalExpr]): ColRef = exprOpt match {
            case Some(expr) => column(expr)
            case None => throw new IllegalArgumentException(
                "Unable to mark as \"" + layer.geom.geomType +
                "\" does not specify an expression for the axis"
            )
        }

        val xcol: ColRef = axisColumn(layer.geom.xAxisExprOpt)
        val ycol: ColRef = axisColumn(layer.geom.yAxisExprOpt)

        layer.marks.foldLeft ((dataExpr, List[Layer]())) {
            case ((prevExpr, prevLayers), (mark, axisIdOpt)) =>
                val (geom, keyCol, targets) = axisIdOpt match {
                    case Some(AxisX) => mark.geomTarget(AxisX, xcol)
                    case Some(AxisY) => mark.geomTarget(AxisY, ycol)
                    case None => mark.geomTarget(xcol, ycol)
                }
        
                val (nextExpr, markLayer) = mark.rewrite(
                    prevExpr, layer.groupOpt.map { e => column(e) },
                    keyCol, geom, targets, facetExprs.map { e => column(e) }
                )

                (nextExpr, prevLayers :+ markLayer)
        }
    }

    private def statsRewrite(
        dataExpr: RelExpr,
        facetExprs: List[ScalExpr],
        layers: List[Layer]
    ): (RelExpr, List[Layer]) = {
        layers.foldLeft (dataExpr, List[Layer]()) {
            case ((prevExpr, prevLayers), layer) =>
                if( layer.stats.isEmpty ) (prevExpr, prevLayers :+ layer) else {
                    val (nextExpr, statLayers) =
                        statsRewrite(prevExpr, facetExprs, layer)

                    val resultLayers: List[Layer] =
                        if( layer.isHidden ) statLayers else {
                            val updLayer: Layer = Layer(
                                layer.geom, layer.aes,
                                layer.groupOpt, layer.keyOpt,
                                layer.posOpt, stats = Nil, layer.marks,
                                layer.tooltipOpt, layer.isHidden,
                                layer.isGenerated, layer.displayOrder
                            )

                            updLayer :: statLayers
                        }

                    val nextLayers: List[Layer] = prevLayers ::: resultLayers

                    (nextExpr, nextLayers)
                }
        }
    }

    private def statsRewrite(
        dataExpr: RelExpr,
        facetExprs: List[ScalExpr],
        layer: Layer
    ): (RelExpr, List[Layer]) =
        layer.stats.foldLeft ((dataExpr, List[Layer]())) {
            case ((prevDataExpr, prevStatLayers), (stat, tasks)) =>
                val (nextDataExpr, statLayer) = stat.rewrite(
                    prevDataExpr, facetExprs, layer, tasks
                )

                val nextStatLayers: List[Layer] = prevStatLayers :+ statLayer

                (nextDataExpr, nextStatLayers)
        }

    private def positionRewrite(
        dataExpr: RelExpr,
        facetExprs: List[ScalExpr],
        layers: List[Layer]
    ): (RelExpr, List[Layer]) =
        layers.reverse.foldLeft (dataExpr, List[Layer]()) {
            case ((prevExpr, prevLayers), layer) =>
                val (nextExpr, nextPosOpt) = layer.posOpt match {
                    case Some(pos) =>
                        val (rewrite, nextPos) =
                            pos.rewrite(prevExpr, layer.geom)
                        (rewrite, Some(nextPos))
                    case None =>
                        (prevExpr, None)
                }

                val nextLayer: Layer = Layer(
                    layer.geom, layer.aes,
                    layer.groupOpt, layer.keyOpt, nextPosOpt,
                    layer.stats, layer.marks, layer.tooltipOpt,
                    layer.isHidden, layer.isGenerated, layer.displayOrder
                )

                (nextExpr, nextLayer::prevLayers)
        }

    private def normalize(
        facet: Facet,
        map: Map[ScalExpr, ColRef]
    ): Facet = {
        val updRowsExprOpt: Option[ScalExpr] =
            facet.rowsExprOpt.map { e => normalize(e, map) }
        val updColsExprOpt: Option[ScalExpr] =
            facet.colsExprOpt.map { e => normalize(e, map) }

        Facet(updRowsExprOpt, updColsExprOpt, facet.margins)
    }
}
