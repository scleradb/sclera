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

package com.scleradb.visual.sql.parser

import scala.language.postfixOps

import com.scleradb.sql.expr.{ScalExpr, ScalValueBase, CharConst, RelExpr}
import com.scleradb.sql.parser.SqlQueryParser

import com.scleradb.visual.model.plot._
import com.scleradb.visual.model.spec._

private[scleradb]
trait PlotParser extends SqlQueryParser {
    lexical.delimiters ++= Seq("(", ")", ",", "=" )
    lexical.reserved ++= Seq(
        "ALIGN", "ALIGNED", "ALPHA", "AXIS",
        "BOTTOM", "COLOR", "COLOUR", "COLUMNS",
        "DISPLAY", "DISTINCT", "DISPLAYORDER", "DOMAIN", "DURATION",
        "EASE", "FACET", "FALSE", "FILL", "FREE", "GEOM", "GRID", "GROUP",
        "HEIGHT", "HIDDEN", "HJUST", "IDENTITY", "INCREASING", "INTERPOLATE",
        "KEY", "LABEL", "LABELS",
        "LAYER", "LAYOUT", "LEFT", "LEGEND", "LINEAR", "LOG",
        "MAP", "MARGIN", "MARK",
        "OFF", "ON", "ORDINAL", "ORIENT", "ORIENTATION",
        "PADDING", "PLOT", "POSITION", "PROJECTION",
        "RANGE", "REVERSED", "RIGHT", "ROWS",
        "SCALE", "SHAPE", "SIZE", "SQRT", "STAT", "STROKE",
        "STROKE_DASHARRAY", "STROKE_OPACITY", "STROKE_WIDTH", "SYMBOL",
        "TENSION", "TICKS", "TIME", "TICKFORMAT", "TITLE", "TOOLTIP", "TOP",
        "TRANSITION", "TRUE", "TYPE", "VJUST",
        "WEIGHT", "WIDTH", "WINDOW", "XAXIS", "YAXIS", "ZOOM"
    )

    def plotSpec: Parser[PlotSpec] =
        dataPlotSetTasks ~ rep(layoutSetTask) ~
        opt("TRANSITION" ~> paren(repsep(transitionSetTask, ","))) ~
        opt("LAYOUT" ~> "ALIGNED") ^^ {
            case dataTasks~layoutTasks~transitionTasksOpt~alignedOpt =>
                PlotSpec(
                    dataTasks, layoutTasks, transitionTasksOpt.toList.flatten,
                    !alignedOpt.isEmpty
                )
        }

    def dataPlotSetTasks: Parser[List[DataPlotSetTask]] =
        rep1(plotTasks) ~ opt(facetTask) ~ rep(axisTask) ^^ {
            case plotTasks~facetTaskOpt~axisTasks =>
                plotTasks.flatten ::: facetTaskOpt.toList ::: axisTasks
        }

    def plotTasks: Parser[List[DataPlotSetSubPlot]] =
        "PLOT" ~> opt(paren(dataPlotSetSubPlots)) ^^ {
            case Some(subPlots) => subPlots
            case None => List(DataPlotSetSubPlot(Nil))
        }

    def facetTask: Parser[DataPlotSetFacet] =
        "FACET" ~> paren(repsep(facetSetTask, ",")) ^^ {
            tasks => DataPlotSetFacet(tasks)
        }

    def axisTask: Parser[DataPlotSetAxis] =
        "AXIS" ~> scalExpr ~ paren(repsep(axisSetTask, ",")) ^^ {
            case expr~tasks => DataPlotSetAxis(expr, tasks)
        }

    def dataPlotSetSubPlots: Parser[List[DataPlotSetSubPlot]] =
        layerSetCrossGeoms ~ opt("," ~> repsep(layerSetExtraTask, ",")) ^^ {
            case geomTasks~tasksOpt => geomTasks.map { geomTask =>
                DataPlotSetSubPlot(
                    List(DataSubPlotSetLayer(geomTask::tasksOpt.getOrElse(Nil)))
                )
            }
        } |
        repsep(dataSubPlotSetLayer, ",") ^^ { tasks =>
            List(DataPlotSetSubPlot(tasks))
        }

    def dataSubPlotSetLayer: Parser[DataSubPlotSetLayer] =
        "LAYER" ~> paren(repsep(layerSetTask, ",")) ^^ {
            tasks => DataSubPlotSetLayer(tasks)
        }

    def layoutSetTask: Parser[LayoutSetTask] =
        "DISPLAY" ~> paren(repsep(displaySetTask, ",")) ^^ {
            tasks => LayoutSetDisplay(tasks)
        } |
        coordSetTask ^^ {
            task => LayoutSetCoordinates(task)
        }

    def displaySetTask: Parser[DisplaySetTask] =
        "WIDTH" ~> equals(rawNumericConst) ^^ {
            width => DisplaySetWidth(width)
        } |
        "HEIGHT" ~> equals(rawNumericConst) ^^ {
            height => DisplaySetHeight(height)
        } |
        "MARGIN" ~> paren(repsep(marginSetTask, ",")) ^^ {
            tasks => DisplaySetMargin(tasks)
        } |
        "LEGEND" ~> paren(repsep(legendDisplaySetTask, ",")) ^^ {
            tasks => DisplaySetLegend(tasks)
        }

    def marginSetTask: Parser[DisplayMarginSetTask] =
        "TOP" ~> equals(signedRawNumericConst) ^^ {
            top => DisplayMarginSetTop(top)
        } |
        "RIGHT" ~> equals(signedRawNumericConst) ^^ {
            right => DisplayMarginSetRight(right)
        } |
        "BOTTOM" ~> equals(signedRawNumericConst) ^^ {
            bottom => DisplayMarginSetBottom(bottom)
        } |
        "LEFT" ~> equals(signedRawNumericConst) ^^ {
            left => DisplayMarginSetLeft(left)
        }

    def legendDisplaySetTask: Parser[LegendDisplaySetTask] =
        "PADDING" ~> equals(signedRawNumericConst) ^^ {
            top => LegendDisplaySetPadding(top)
        } |
        "WIDTH" ~> equals(rawNumericConst) ^^ {
            width => LegendDisplaySetWidth(width)
        }

    def coordSetTask: Parser[CoordSetTask] =
        "GRID" ~> paren(repsep(gridSetTask, ",")) ^^ {
            tasks => CoordSetGrid(tasks)
        } |
        "MAP" ~> paren(repsep(mapSetTask, ",")) ^^ {
            tasks => CoordSetMap(tasks)
        }

    def gridSetTask: Parser[GridSetTask] =
        "TYPE" ~> equals(charConstOrIdent) ^^ {
            gridType => GridSetType(gridType)
        } |
        gridAesSetTask ^^ {
            task => GridSetAes(List(task))
        }

    def gridAesSetTask: Parser[GridAesSetTask] =
        ("COLOR" | "COLOUR") ~> equals(rawCharConst) ^^ {
            color => GridAesSetColor(color)
        } |
        axisId ~ paren(repsep(axisAesSetTask, ",")) ^^ {
            case axisId~tasks => GridAesSetAxis(axisId, tasks)
        }

    def axisId: Parser[AxisId] =
        "XAXIS" ^^^ { AxisX } | "YAXIS" ^^^ { AxisY }

    def axisSetTask: Parser[AxisSetTask] =
        "LABEL" ~> equals(rawCharConst) ^^ {
            label => AxisSetLabel(label)
        } |
        "SCALE" ~> equals(axisScale) ^^ {
            scale => AxisSetScale(scale)
        } |
        "FREE" ~> opt(equals(state)) ^^ {
            isFreeOpt => AxisSetFree(isFreeOpt getOrElse true)
        } |
        "INCREASING" ~> opt(equals(state)) ^^ {
            isIncreasingOpt => AxisSetIncreasing(isIncreasingOpt getOrElse true)
        } |
        "ZOOM" ~> opt(equals(state)) ^^ {
            isZoomOpt => AxisSetZoom(isZoomOpt getOrElse true)
        } |
        "WINDOW" ~> equals(rawIntConst) ^^ {
            windowSize => AxisSetWindowSize(windowSize)
        } |
        "TICKFORMAT" ~> equals(rawCharConst) ^^ {
            tickFormat => AxisSetTickFormat(tickFormat)
        } |
        "TICKS" ~> equals(rawIntConst) ^^ {
            ticks => AxisSetTicks(ticks)
        } |
        "WEIGHT" ~> equals(rawNumericConst) ^^ {
            weight => AxisSetWeight(weight)
        }

    def axisAesSetTask: Parser[AxisAesSetTask] =
        ("COLOR" | "COLOUR") ~> equals(rawCharConst) ^^ {
            color => AxisAesSetColor(color)
        } |
        "TICKS" ~> equals(rawCharConst) ^^ {
            color => AxisAesSetTicks(color)
        }

    def axisOrient: Parser[String] = "LEFT" | "RIGHT" | "TOP" | "BOTTOM"

    def mapSetTask: Parser[MapSetTask] =
        "PROJECTION" ~> equals(
            charConstOrIdent ~
            opt(paren(signedRawNumericConst ~ ("," ~> signedRawNumericConst)))
        ) ^^ {
            case name~Some(a~b) => MapSetProjection(name, Some((a, b)))
            case name~None => MapSetProjection(name, None)
        } |
        ("ORIENT" | "ORIENTATION") ~> equals(paren(
            signedRawNumericConst ~
            ("," ~> signedRawNumericConst) ~ ("," ~> signedRawNumericConst)
        )) ^^ {
            case x~y~z => MapSetOrientation(x, y, z)
        }

    def facetSetTask: Parser[FacetSetTask] =
        "ROWS" ~> equals(scalExpr) ^^ {
            expr => FacetSetRows(expr)
        } |
        "COLUMNS" ~> equals(scalExpr) ^^ {
            expr => FacetSetColumns(expr)
        } |
        "MARGIN" ~> opt(equals(state)) ^^ {
            stateOpt => FacetSetMargin(stateOpt getOrElse true)
        }

    def transitionSetTask: Parser[TransitionSetTask] =
        "DURATION" ~> equals(rawIntConst) ^^ {
            duration => TransitionSetDuration(duration)
        } |
        "EASE" ~> equals(rawCharConst) ^^ {
            ease => TransitionSetEase(ease)
        }

    def layerSetTask: Parser[LayerSetTask] = layerSetGeom | layerSetExtraTask

    def layerSetGeom: Parser[LayerSetGeom] =
        "GEOM" ~> equals(
            charConstOrIdent ~ opt(paren(repsep(scalExprParam, ",")))
        ) ^^ {
            case geom~paramsOpt => LayerSetGeom(geom, paramsOpt.getOrElse(Nil))
        }
        
    def layerSetCrossGeoms: Parser[List[LayerSetGeom]] =
        "GEOM" ~> equals(
            charConstOrIdent ~ opt(paren(repsep(scalExprListParam, ",")))
        ) ^^ {
            case geom~paramsListOpt =>
                unroll(paramsListOpt.getOrElse(Nil)).map { params =>
                    LayerSetGeom(geom, params)
                }
        }

    def scalExprListParam: Parser[(String, List[ScalExpr])] =
        charConstOrIdent ~ equals(scalExprOrListPar) ^^ {
            case prop~exprs => (prop, exprs)
        }

    def layerSetExtraTask: Parser[LayerSetTask] =
        aesProp ~ aesSetTasks ^^ {
            case prop~tasks => LayerSetAes(prop, tasks)
        } |
        "GROUP" ~> equals(scalExpr) ^^ {
            expr => LayerSetGroup(expr)
        } |
        "KEY" ~> equals(scalExpr) ^^ {
            expr => LayerSetKey(expr)
        } |
        "POSITION" ~> equals(
            charConstOrIdent ~ opt(paren(repsep(posParam, ","))) ~
            opt("ORDER" ~> "BY" ~> orderedScalExprOrListPar)
        ) ^^ {
            case pos~paramsOpt~orderOpt =>
                LayerSetPos(
                    pos, paramsOpt.getOrElse(Nil), orderOpt.getOrElse(Nil)
                )
        } |
        "STAT" ~> equals(
            charConstOrIdent ~ opt(paren(repsep(statParam, ",")))
        ) ^^ {
            case stat~paramTasksOpt =>
                val (params, statTasks) = paramTasksOpt.getOrElse(Nil).unzip
                LayerSetStat(stat, params.flatten, statTasks.flatten)
        } |
        "MARK" ~> opt(axisId) ~
        paren(scalExpr ~ opt("," ~> rep1sep(layerSetTask, ","))) ^^ {
            case axisIdOpt~(pred~tasksOpt) =>
                LayerSetMark(axisIdOpt, pred, tasksOpt.getOrElse(Nil))
        } |
        "TOOLTIP" ~> equals(layerSetTooltip) |
        "HIDDEN" ~> opt(equals(state)) ^^ {
            isHiddenOpt => LayerSetHidden(isHiddenOpt getOrElse true)
        } |
        "DISPLAYORDER" ~> equals(signedRawNumericConst) ^^ {
            dispOrder => LayerSetDisplayOrder(dispOrder)
        }

    def layerSetTooltip: Parser[LayerSetTask] =
        scalExpr ^^ { expr => LayerSetTooltip(expr) } |
        aesProp ^^ { prop => LayerCopyTooltip(prop) }

    def statParam: Parser[(Option[(String, ScalExpr)], Option[LayerSetTask])] =
        scalExprParam ^^ {
            param => (Some(param), None)
        } |
        layerSetTask ^^ {
            statTask => (None, Some(statTask))
        }

    def scalExprParam: Parser[(String, ScalExpr)] =
        charConstOrIdent ~ equals(scalExpr) ^^ {
            case prop~expr => prop -> expr
        }

    def posParam: Parser[(String, Double)] =
        ("PADDING" | ident) ~ equals(signedRawNumericConst) ^^ {
            case prop~value => prop -> value
        }

    def aesProp: Parser[String] =
        "SIZE" | "SHAPE" | "FILL" | "ALPHA" | "STROKE" |
        "STROKE_DASHARRAY" ^^^ { "STROKE-DASHARRAY" } |
        "STROKE_WIDTH" ^^^ { "STROKE-WIDTH" } |
        "STROKE_OPACITY" ^^^ { "STROKE-OPACITY" } |
        "INTERPOLATE" | "TENSION" | "HJUST" | "VJUST"

    def aesSetTasks: Parser[List[AesSetTask]] =
        equals(aesSetValue) ~ rep(aesSetTask) ^^ {
            case setValueTasks~tasks => setValueTasks ::: tasks
        }

    def aesSetValue: Parser[List[AesSetTask]] =
        "IDENTITY" ~> paren(scalExpr) ^^ {
            expr => List(AesSetValue(expr), AesSetIdentityScale)
        } |
        "DISTINCT" ~> paren(scalExpr) ^^ { expr => List(AesSetValue(expr)) } |
        scalExpr ^^ { expr => List(AesSetValue(expr)) } |
        aesProp ^^ { prop => List(AesCopyValue(prop)) }

    def aesSetTask: Parser[AesSetTask] =
        "LEGEND" ~> opt(paren(repsep(legendSetTask, ","))) ^^ {
            tasksOpt => AesSetLegend(tasksOpt.getOrElse(Nil))
        } |
        "SCALE" ~> equals(aesScale) ^^ {
            scaleSpec => AesSetScale(scaleSpec)
        } |
        opt("ON") ~> "NULL" ~> scalValueBaseElem ^^ { nv => AesSetOnNull(nv) }

    def legendSetTask: Parser[LegendSetTask] =
        "ORIENTATION" ~> equals(charConstOrIdent) ^^ {
            orient => LegendSetOrientation(orient)
        } |
        "TITLE" ~> equals(rawCharConst) ^^ {
            title => LegendSetTitle(title)
        } |
        "LABELS" ~> equals(paren(repsep(legendLabel, ","))) ^^ {
            labels => LegendSetLabels(LegendLabels(labels))
        } |
        "ALIGN" ~> equals(charConstOrIdent) ^^ {
            align => LegendSetLabelAlign(align)
        } |
        "REVERSED" ^^^ { LegendSetLabelOrder(isReversed = true) }

    def legendLabel: Parser[(ScalValueBase, Option[String])] =
        scalValueBaseElem ~ opt("AS" ~> rawCharConst) ^^ {
            case k~vOpt => (k, vOpt)
        }

    def aesScale: Parser[Scale] = axisScale | colorScale | symbolScale

    def axisScale: Parser[Scale] = quantScale | ordinalScale

    def quantScale: Parser[QuantScale] =
        ("TIME" | "LINEAR" | "LOG" | "SQRT") ~
        opt(paren(quantOptParams)) ^^ {
            case name~Some((domainOpt, rangeOpt)) =>
                Scale.quant(name.toUpperCase, domainOpt, rangeOpt)
            case name~None =>
                Scale.quant(name.toUpperCase, None, None)
        }

    def quantOptParams: Parser[(
            Option[(Option[ScalValueBase], Option[ScalValueBase])],
            Option[(Option[ScalValueBase], Option[ScalValueBase])]
    )] =
        "DOMAIN" ~> equals(paren(quantParams)) ~
        opt(opt(",") ~> "RANGE" ~> equals(paren(quantParams))) ^^ {
            case domain~rangeOpt => (Some(domain), rangeOpt)
        } |
        "RANGE" ~> equals(paren(quantParams)) ^^ {
            range => (None, Some(range))
        } |
        quantParams ^^ {
            domain => (Some(domain), None)
        }

    def quantParams: Parser[(Option[ScalValueBase], Option[ScalValueBase])] =
        scaleParamOpt ~ ("," ~> scaleParamOpt) ^^ { case a~b => (a, b) }

    def ordinalScale: Parser[OrdinalScale] =
        "ORDINAL" ~> opt(paren(ordinalScaleOps)) ^^ {
            case Some(scale) => scale
            case None => Scale.ordinal
        }

    def ordinalScaleOps: Parser[OrdinalScale] =
        opt("DOMAIN" ~> equals(paren(ordinalParams)) <~ ",") ~
        ("RANGE" ~> equals(paren(ordinalParams))) ^^ {
            case domainOpt~range => Scale.ordinal(domainOpt, range)
        } |
        ordinalParams ^^ {
            range => Scale.ordinal(None, range)
        }

    def ordinalParams: Parser[List[ScalValueBase]] =
        rep1sep(scaleParamOpt, ",") ^^ { vs => vs.flatten }

    def colorScale: Parser[ColorScale] =
        ("COLOR" | "COLOUR") ~> opt(paren(colorScaleOps)) ^^ {
            case Some(spec) => spec
            case None => Scale.color(None, Nil)
        }

    def colorScaleOps: Parser[ColorScale] =
        opt("DOMAIN" ~> equals(paren(ordinalParams)) <~ ",") ~
        ("RANGE" ~> equals(paren(colorRangeParams))) ^^ {
            case domainOpt~range => Scale.color(domainOpt, range)
        } |
        colorParams

    def colorRangeParams: Parser[List[String]] =
        rep1sep(rawCharConst, ",")

    def colorParams: Parser[ColorScale] =
        rep1sep(scaleParamOpt ~ opt("AS" ~> rawCharConst), ",") ^^ { ps =>
            val (kOpts, vOpts) =
                ps.map { case kOpt~vOpt => (kOpt, vOpt) } unzip

            if( vOpts.flatten.isEmpty ) {
                val range: List[String] = kOpts.flatten.map {
                    case CharConst(v) => v
                    case other =>
                        throw new IllegalArgumentException(
                            "Invalid color: " + other.repr
                        )
                }

                Scale.color(None, range)
            } else {
                val domain: List[ScalValueBase] = kOpts.map {
                    case Some(v) => v
                    case None =>
                        throw new IllegalArgumentException(
                            "Cannot map a NULL value"
                        )
                }

                val range: List[String] =
                    vOpts.map { vOpt => vOpt getOrElse "black" }

                Scale.color(Some(domain), range)
            }
        }

    def symbolScale: Parser[SymbolScale] =
        "SYMBOL" ~> opt(paren(symbolScaleOps)) ^^ {
            case Some(spec) => spec
            case None => Scale.symbol(None, Nil)
        }

    def symbolScaleOps: Parser[SymbolScale] =
        opt("DOMAIN" ~> equals(paren(ordinalParams)) <~ ",") ~
        ("RANGE" ~> equals(paren(symbolRangeParams))) ^^ {
            case domainOpt~range => Scale.symbol(domainOpt, range)
        } |
        symbolParams

    def symbolRangeParams: Parser[List[String]] =
        rep1sep(rawCharConst, ",")

    def symbolParams: Parser[SymbolScale] =
        rep1sep(scaleParamOpt ~ opt("AS" ~> rawCharConst), ",") ^^ { ps =>
            val (kOpts, vOpts) =
                ps.map { case kOpt~vOpt => (kOpt, vOpt) } unzip

            if( vOpts.flatten.isEmpty ) {
                val range: List[String] = kOpts.flatten.map {
                    case CharConst(v) => v
                    case other =>
                        throw new IllegalArgumentException(
                            "Invalid symbol: " + other.repr
                        )
                }

                Scale.symbol(None, range)
            } else {
                val domain: List[ScalValueBase] = kOpts.map {
                    case Some(v) => v
                    case None =>
                        throw new IllegalArgumentException(
                            "Cannot map a NULL value"
                        )
                }

                val range: List[String] =
                    vOpts.map { vOpt => vOpt getOrElse "circle" }

                Scale.symbol(Some(domain), range)
            }
        }

    def scaleParamOpt: Parser[Option[ScalValueBase]] =
        scalExpr ^^ {
            expr => scalExprEvaluator.eval(expr) match {
                case (v: ScalValueBase) => Some(v)
                case _ => None
            }
        }

    def charConstOrIdent: Parser[String] =
        rawCharConst ^^ { s => s.toUpperCase } | ident

    def state: Parser[Boolean] =
        ("ON" | "TRUE") ^^^ { true } | ("OFF" | "FALSE") ^^^ { false }

    def equals[T](p: Parser[T]): Parser[T] = "=" ~> p
    def paren[T](p: Parser[T]): Parser[T] = "(" ~> p <~ ")"

    private def unroll(
        params: List[(String, List[ScalExpr])]
    ): List[List[(String, ScalExpr)]] = params match {
        case (prop, vs)::remParams =>
            unroll(remParams).flatMap { ls => vs.map { v => (prop, v)::ls } }

        case Nil => List(Nil)
    }
}
