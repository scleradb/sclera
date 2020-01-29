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
import scala.math.Ordering.Double.TotalOrdering

import play.api.libs.json._

import com.scleradb.util.tools.Counter
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableRow
import com.scleradb.sql.types._
import com.scleradb.sql.expr._

import com.scleradb.visual.model.plot._

object ResultJson {
    def text(s: String): JsObject = jsObject(
        "type" -> JsString("text"),
        "text" -> JsString(s)
    )

    def table(cols: Seq[Column], titleOpt: Option[String]): JsObject = jsObject(
        "type" -> JsString("table"),
        "columns" -> columnSpec(cols),
        "title" -> titleOpt.map(s => JsString(s)).getOrElse(JsNull)
    )

    def plot(
        layout: Layout,
        trans: Transition,
        facetOpt: Option[Facet],
        plotInfo: DataPlotInfo,
        cols: Seq[Column],
        titleOpt: Option[String]
    ): JsObject = jsObject(
        "type" -> JsString("plot"),
        "plot" -> plotSpec(
            layout, trans, facetOpt, plotInfo,
            cols.map { col => ColRef(col.name) }
        ),
        "columns" -> columnSpec(cols),
        "title" -> titleOpt.map(s => JsString(s)).getOrElse(JsNull)
    )

    private def plotSpec(
        layout: Layout,
        trans: Transition,
        facetOpt: Option[Facet],
        plotInfo: DataPlotInfo,
        cols: Seq[ColRef]
    ): JsObject = jsObject(
        "name" -> JsString(Counter.nextSymbol("PLOT")),
        "layout" -> toJson(layout),
        "trans" -> toJson(trans),
        "axes" -> jsObject(
            "x" -> jsArraySeq(plotInfo.xAxes.map {
                case (axis, length) => toJson(axis, length)
            }),
            "y" -> jsArraySeq(plotInfo.yAxes.map {
                case (axis, length) => toJson(axis, length)
            })
        ),
        "facet" -> facetOpt.map(facet => toJson(facet, cols)).getOrElse(JsNull),
        "subplots" -> jsArraySeq(
            plotInfo.subPlots.map {
                case (d, ((xAxisIndex, yAxisIndex), layers)) => jsObject(
                    "display" -> jsObject(
                        "x" -> JsNumber(d.x),
                        "y" -> JsNumber(d.y),
                        "width" -> JsNumber(d.width),
                        "height" -> JsNumber(d.height)
                    ),
                    "axis" -> jsObject(
                        "x" -> JsNumber(xAxisIndex),
                        "y" -> JsNumber(yAxisIndex)
                    ),
                    "layers" -> jsArraySeq(
                        layers.sortBy { layer => layer.displayOrder } map {
                            layer => toJson(layer, cols)
                        }
                    )
                )
            }
        )
    )

    private def columnSpec(cols: Seq[Column]): JsArray = jsArraySeq(
        cols.map { col => toJson(col) }
    )

    def data(cols: Seq[Column], rows: Seq[TableRow]): JsObject = jsObject(
        "type" -> JsString("data"),
        "data" -> jsArraySeq(rows.map { r =>
            jsArraySeq(cols.map { col => toJsonVal(r.getScalExpr(col)) })
        })
    )

    private def toJson(layout: Layout): JsObject = jsObject(
        "coord" -> toJson(layout.coord),
        "display" -> toJson(layout.display)
    )

    private def toJson(coord: Coordinates): JsObject = coord match {
        case CoordGrid(gridType, aes) => jsObject(
            "type" -> JsString("grid"),
            "grid" -> JsString(toString(gridType)),
            "aes" -> jsObject(
                "color" -> JsString(aes.color),
                "x" -> toJson(aes.xAxisAes),
                "y" -> toJson(aes.yAxisAes)
            )
        )

        case CoordMap(projection, MapOrient(x, y, z)) => jsObject(
            "type" -> JsString("map"),
            "projection" -> toJson(projection),
            "orient" -> jsArraySeq(Seq(x, y, z).map(v => JsNumber(v)))
        )
    }

    private def toString(gridType: GridType): String = gridType match {
        case Cartesian => "cartesian"
        case Polar => "polar"
    }

    private def toJson(axis: Axis, length: Double): JsObject = jsObject(
        "label" ->
            axis.labelOpt.map(label => JsString(label)).getOrElse(JsNull),
        "orient" -> JsString(toString(axis.orient)),
        "scale" -> toJson(axis.scale),
        "isfree" -> JsBoolean(axis.isFree),
        "isincreasing" -> JsBoolean(axis.isIncreasing),
        "iszoomin" -> JsBoolean(axis.isZoomIn),
        "windowsize" ->
            axis.windowSizeOpt.map(v => JsNumber(v)).getOrElse(JsNull),
        "tickformat" ->
            axis.tickFormatOpt.map(f => JsString(f)).getOrElse(JsNull),
        "ticks" -> axis.ticksOpt.map(n => JsNumber(n)).getOrElse(JsNull),
        "length" -> JsNumber(length)
    )

    private def toString(orient: AxisOrient): String = orient match {
        case AxisLeft => "left"
        case AxisRight => "right"
        case AxisTop => "top"
        case AxisBottom => "bottom"
    }

    private def toJson(aes: AxisAes): JsObject = jsObject(
        "color" -> JsString(aes.color),
        "ticks" -> JsString(aes.ticks)
    )

    private def toJson(proj: MapProject): JsObject = proj match {
        case AzimuthalEqualArea =>
            jsObject("type" -> JsString("azimuthalEqualArea"))
        case AzimuthalEquiDistant =>
            jsObject("type" -> JsString("azimuthalEquidistant"))
        case ConicConformal(a, b) => jsObject(
            "type" -> JsString("conicConformal"),
            "parallels" -> jsArray(JsNumber(a), JsNumber(b))
        )
        case ConicEquiDistant(a, b) => jsObject(
            "type" -> JsString("conicEquiDistant"),
            "parallels" -> jsArray(JsNumber(a), JsNumber(b))
        )
        case EquiRectangular =>
            jsObject("type" -> JsString("equiRectangular"))
        case Mercator =>
            jsObject("type" -> JsString("mercator"))
        case Orthographic =>
            jsObject("type" -> JsString("orthographic"))
        case Stereographic =>
            jsObject("type" -> JsString("stereographic"))
        case TransverseMercator =>
            jsObject("type" -> JsString("transverseMercator"))
    }

    private def toJson(display: Display): JsObject = jsObject(
        "width" -> JsNumber(display.width),
        "height" -> JsNumber(display.height),
        "margin" -> toJson(display.margin),
        "legend" -> toJson(display.legendDisplay, display.height)
    )

    private def toJson(margin: DisplayMargin): JsObject = jsObject(
        "top" -> JsNumber(margin.top),
        "right" -> JsNumber(margin.right),
        "bottom" -> JsNumber(margin.bottom),
        "left" -> JsNumber(margin.left)
    )

    private def toJson(
        legend: LegendDisplay,
        height: Double
    ): JsObject = jsObject(
        "padding" -> JsNumber(legend.padding),
        "width" -> JsNumber(legend.width),
        "height" -> JsNumber(height)
    )

    private def toJson(scale: Scale): JsObject = scale match {
        case TimeScale((minOpt, maxOpt), rangeOpt) => jsObject(
            "type" -> JsString("time"),
            "min" -> (minOpt.map { v => toJsonVal(v) } getOrElse JsNull),
            "max" -> (maxOpt.map { v => toJsonVal(v) } getOrElse JsNull),
            "range" -> (rangeOpt.map {
                case (a, b) => jsArray(JsNumber(a), JsNumber(b))
            } getOrElse JsNull)
        )

        case LinearScale((minOpt, maxOpt), rangeOpt) => jsObject(
            "type" -> JsString("linear"),
            "min" -> (minOpt.map { v => toJsonVal(v) } getOrElse JsNull),
            "max" -> (maxOpt.map { v => toJsonVal(v) } getOrElse JsNull),
            "range" -> (rangeOpt.map {
                case (a, b) => jsArray(JsNumber(a), JsNumber(b))
            } getOrElse JsNull)
        )

        case LogScale((minOpt, maxOpt), rangeOpt) => jsObject(
            "type" -> JsString("log"),
            "min" -> (minOpt.map { v => toJsonVal(v) } getOrElse JsNull),
            "max" -> (maxOpt.map { v => toJsonVal(v) } getOrElse JsNull),
            "range" -> (rangeOpt.map {
                case (a, b) => jsArray(JsNumber(a), JsNumber(b))
            } getOrElse JsNull)
        )

        case SqrtScale((minOpt, maxOpt), rangeOpt) => jsObject(
            "type" -> JsString("sqrt"),
            "min" -> (minOpt.map { v => toJsonVal(v) } getOrElse JsNull),
            "max" -> (maxOpt.map { v => toJsonVal(v) } getOrElse JsNull),
            "range" -> (rangeOpt.map {
                case (a, b) => jsArray(JsNumber(a), JsNumber(b))
            } getOrElse JsNull)
        )

        case OrdinalValueScale(domainOpt, range, isBand) => jsObject(
            "type" -> JsString("ordinal"),
            // For D3.V5, use the band scale when isBand is true
            // "type" -> JsString(if( isBand ) "band" else "ordinal"),
            "domain" -> (domainOpt.map { vs =>
                jsArraySeq(vs.map { v => toJsonVal(v) })
            } getOrElse JsNull),
            "range" -> jsArraySeq(range.map { v => toJsonVal(v) })
        )

        case ColorScale(None, Nil) => jsObject(
            "type" -> JsString("color"),
            "scheme" -> JsString("category20")
        )

        case ColorScale(None, List(scheme)) => jsObject(
            "type" -> JsString("color"),
            "scheme" -> JsString(scheme)
        )

        case ColorScale(domainOpt, range) => jsObject(
            "type" -> JsString("color"),
            "domain" -> (domainOpt.map { vs =>
                jsArraySeq(vs.map { v => toJsonVal(v) })
            } getOrElse JsNull),
            "range" -> jsArraySeq(range.map { v => JsString(v) })
        )

        case SymbolScale(domainOpt, Nil) => jsObject(
            "type" -> JsString("symbol"),
            "domain" -> (domainOpt.map { vs =>
                jsArraySeq(vs.map { v => toJsonVal(v) })
            } getOrElse JsNull)
        )

        case SymbolScale(domainOpt, range) => jsObject(
            "type" -> JsString("symbol"),
            "domain" -> (domainOpt.map { vs =>
                jsArraySeq(vs.map { v => toJsonVal(v) })
            } getOrElse JsNull),
            "range" -> jsArraySeq(range.map { v => JsString(v) })
        )

        case IdentityScaleOrdinal => jsObject(
            "type" -> JsString("identityord")
        )

        case IdentityScaleQuant => jsObject(
            "type" -> JsString("identityquant")
        )
    }

    private def toJson(
        facet: Facet,
        cols: Seq[ColRef]
    ): JsObject = jsObject(
        "rows" -> facet.rowsExprOpt.map(e => toJson(e, cols)).getOrElse(JsNull),
        "cols" -> facet.colsExprOpt.map(e => toJson(e, cols)).getOrElse(JsNull),
        "margins" -> JsBoolean(facet.margins)
    )

    private def toJson(trans: Transition): JsObject = jsObject(
        "duration" -> JsNumber(trans.duration),
        "ease" -> JsString(trans.ease),
        "tooltip" -> jsObject(
            "fadein" -> JsNumber(200),
            "fadeout" -> JsNumber(500)
        )
    )

    private def toJson(
        layer: Layer,
        cols: Seq[ColRef]
    ): JsObject = jsObject(
        "type" -> JsString(layer.geom.geomType),
        "name" -> JsString(Counter.nextSymbol("LAYER")),
        "config" -> jsObject(
            "group" ->
                layer.groupOpt.map(v => toJson(v, cols)).getOrElse(JsNull),
            "geom" -> jsObjectSeq(
                layer.geom.propMap.view.mapValues(v => toJson(v, cols)).toSeq
            ),
            "offset" ->
                layer.posOpt.map(pos => toJson(pos, cols)).getOrElse(JsNull),
            "aes" -> jsObjectSeq(
                layer.geom.aesProps.map(attr =>
                    attr -> toJson(layer.aes.propMap(attr), cols)
                ).toSeq
            ),
            "key" -> layer.keyOpt.map(e => toJson(e, cols)).getOrElse(JsNull),
            "tooltip" ->
                layer.tooltipOpt.map(t => toJson(t, cols)).getOrElse(JsNull)
        )
    )

    private def toJson(
        aesElem: AesElement,
        cols: Seq[ColRef]
    ): JsValue = aesElem match {
        case AesConst(v) => toJsonVal(v)

        case AesExpr(col: ColRef, scale, onNullOpt, legendOpt) => jsObject(
            "index" -> JsNumber(colIndex(col, cols)),
            "scale" -> toJson(scale),
            "onnull" -> onNullOpt.map(nv => toJsonVal(nv)).getOrElse(JsNull),
            "legend" -> legendOpt.map(l => toJson(l)).getOrElse(JsNull)
        )

        case AesExpr(e, _, _, _) =>
            throw new IllegalArgumentException(
                "Cannot serialize aesthetics expression: " + e.repr
            )
    }

    private def toJson(
        pos: Position,
        cols: Seq[ColRef]
    ): JsObject = pos match {
        case PositionOffset(props, isRelative) =>
            jsObjectSeq(
                props.map { case (name, col) =>
                    name -> jsObject(
                        "index" -> JsNumber(colIndex(col, cols)),
                        "isrelative" ->
                            JsBoolean(isRelative.get(name) getOrElse false)
                    )
                }
            )

        case _ =>
            throw new IllegalArgumentException(
                "Found unnormalized position, expecting position offsets"
            )
    }

    private def toJson(legend: Legend): JsObject = jsObject(
        "orient" -> toJson(legend.orient),
        "title" -> legend.titleOpt.map(t => JsString(t)).getOrElse(JsNull),
        "labels" ->
            legend.labelsOpt.map(labels => toJson(labels)).getOrElse(JsNull),
        "isreversed" -> JsBoolean(legend.isReversed)
    )

    private def toJson(orient: LegendOrient): JsObject = orient match {
        case LegendVertical => jsObject("type" -> JsString("vertical"))
        case LegendHorizontal(align) => jsObject(
            "type" -> JsString("horizontal"),
            "align" -> JsString(toString(align))
        )
    }

    private def toString(align: LabelAlign): String = align match {
        case LabelStart => "start"
        case LabelMiddle => "middle"
        case LabelEnd => "end"
    }

    private def toJson(labels: LegendLabels): JsObject = labels match {
        case LegendLabelList(labelList) => jsObject(
            "list" -> jsArraySeq(labelList.map { label => JsString(label) })
        )

        case LegendLabelMap(labelMap) => jsObject(
            "map" -> jsArraySeq(
                labelMap.map { case (value, label) =>
                    jsObject(
                        "value" -> toJsonVal(value),
                        "label" -> JsString(label)
                    )
                }
            )
        )
    }

    private def toJson(col: Column): JsObject = jsObject(
        "name" -> JsString(col.name),
        "type" -> JsString(toString(col.sqlType))
    )

    private def toString(sqlType: SqlType): String = sqlType match {
        case SqlOption(t) => toString(t)
        case SqlBool => "boolean"
        case SqlBigInt | SqlInteger | SqlSmallInt |
             SqlDecimal(_, _) | SqlFloat(_) | SqlReal => "numeric"
        case SqlText | SqlCharVarying(_) | SqlCharFixed(_) => "string"
        case SqlTime => "time"
        case SqlDate => "date"
        case SqlTimestamp => "datetime"
        case SqlNullType => "null"
        case _ =>
            throw new IllegalArgumentException(
                "Cannot serialize type \"" + sqlType.repr + "\""
            )
    }

    private def toJson(e: ScalExpr, cols: Seq[ColRef]): JsValue = e match {
        case (col: ColRef) => jsObject(
            "index" -> JsNumber(colIndex(col, cols))
        )
        case (v: ScalColValue) => toJsonVal(v)
        case _ =>
            throw new IllegalArgumentException(
                "Cannot serialize expression: " + e.repr
            )
    }

    private def toJsonVal(v: ScalColValue): JsValue = v match {
        case (_: SqlNull) => JsNull
        case CharConst(v) => JsString(v)
        case BoolConst(v) => JsBoolean(v)
        case (v: NumericConst) => JsNumber(v.numericValue)
        case (v: DateTimeConst) => JsNumber(v.value.getTime)
        case _ =>
            throw new IllegalArgumentException(
                "Cannot serialize value \"" + v.repr + "\""
            )
    }

    private def colIndex(col: ColRef, cols: Seq[ColRef]): Int = {
        val index: Int = cols indexOf col
        if( index < 0 )
            throw new IllegalArgumentException(
                "Cannot find col \"" + col.repr + "\" in " +
                cols.map(c => "\"" + c.repr + "\"").mkString(", ")
            )

        index
    }

    private def jsObject(vs: (String, JsValue)*): JsObject =
        jsObjectSeq(vs.toSeq)
    private def jsObjectSeq(vs: Seq[(String, JsValue)]): JsObject =
        JsObject(vs.filter { case (k, v) => v != JsNull })

    private def jsArray(vs: JsValue*): JsArray = jsArraySeq(vs.toSeq)
    private def jsArraySeq(vs: Seq[JsValue]): JsArray = JsArray(vs)
}
