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

import com.scleradb.sql.expr._

/** Geom paramters */
sealed abstract class Geom {
    val geomType: String

    val defaultAes: Map[String, AesElement]
    def aesProps: List[String] = defaultAes.keys.toList.sorted

    val propMap: Map[String, ScalExpr]

    def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr]
    ): Option[Position]

    def positionOpt(
        posName: String,
        params: List[(String, Double)],
        order: List[SortExpr]
    ): Option[Position] = positionOpt((posName, Map() ++ params), order)

    def scalExprs: List[ScalExpr] = propMap.values.toList

    def xAxisExprOpt: Option[ScalExpr]
    def yAxisExprOpt: Option[ScalExpr]

    def axisExprOpt(axisId: AxisId): Option[ScalExpr] = axisId match {
        case AxisX => xAxisExprOpt
        case AxisY => yAxisExprOpt
    }
}

object Geom {
    lazy val default: Geom = point(ColRef("X"), ColRef("Y"))

    def blank: Geom = Blank

    def point(
        x: ScalExpr,
        y: ScalExpr
    ): Point = Point(x, y)

    def line(
        x: ScalExpr,
        y: ScalExpr
    ): Line = Line(x, y, false, false)

    def pointRangeY(
        x: ScalExpr,
        y: ScalExpr,
        ymin: ScalExpr,
        ymax: ScalExpr
    ): PointRangeY = PointRangeY(x, y, ymin, ymax)

    def rangeY(
        x: ScalExpr,
        ymin: ScalExpr,
        ymax: ScalExpr,
        widthOpt: Option[ScalExpr]
    ): RangeY = RangeY(x, ymin, ymax, widthOpt)

    def rangeX(
        xmin: ScalExpr,
        xmax: ScalExpr,
        y: ScalExpr,
        heightOpt: Option[ScalExpr]
    ): RangeX = RangeX(xmin, xmax, y, heightOpt)

    def ribbon(
        x: ScalExpr,
        ymin: ScalExpr,
        ymax: ScalExpr
    ): Ribbon = Ribbon(x, ymin, ymax)

    def abLine(
        yIntercept: ScalExpr,
        slope: ScalExpr
    ): ABLine = ABLine(yIntercept, slope)

    def hLine(
        y: ScalExpr
    ): ABLine = abLine(y, DoubleConst(0))

    def vLine(
        x: ScalExpr
    ): VLine = VLine(x)

    def segment(
        x: ScalExpr,
        xend: ScalExpr,
        y: ScalExpr,
        yend: ScalExpr
    ): Segment = Segment(x, xend, y, yend, false)

    def arrow(
        x: ScalExpr,
        xend: ScalExpr,
        y: ScalExpr,
        yend: ScalExpr
    ): Segment = Segment(x, xend, y, yend, true)

    def polygon(
        x: ScalExpr,
        y: ScalExpr
    ): Line = Line(x, y, true, false)

    def area(
        x: ScalExpr,
        y: ScalExpr
    ): Line = Line(x, y, false, true)

    def bar(
        x: ScalExpr,
        y: ScalExpr
    ): Bar = Bar(x, y)

    def rect(
        xmin: ScalExpr,
        xmax: ScalExpr,
        ymin: ScalExpr,
        ymax: ScalExpr
    ): Rect = Rect(xmin, xmax, ymin, ymax)

    def hist(
        xmin: ScalExpr,
        xmax: ScalExpr,
        freq: ScalExpr
    ): Rect = Rect(xmin, xmax, DoubleConst(0), freq)

    def regionX(
        min: ScalExpr,
        max: ScalExpr,
        beginOpt: Option[ScalExpr],
        endOpt: Option[ScalExpr]
    ): RegionX = RegionX(min, max, beginOpt, endOpt)

    def regionY(
        min: ScalExpr,
        max: ScalExpr,
        beginOpt: Option[ScalExpr],
        endOpt: Option[ScalExpr]
    ): RegionY = RegionY(min, max, beginOpt, endOpt)

    def region(
        axisId: AxisId,
        min: ScalExpr,
        max: ScalExpr,
        beginOpt: Option[ScalExpr] = None,
        endOpt: Option[ScalExpr] = None
    ): Region1D = axisId match {
        case AxisX => regionX(min, max, beginOpt, endOpt)
        case AxisY => regionY(min, max, beginOpt, endOpt)
    }

    def boxPlot(
        lower: ScalExpr,
        middle: ScalExpr,
        upper: ScalExpr,
        x: ScalExpr,
        ymin: ScalExpr,
        ymax: ScalExpr,
        outlierSize: ScalExpr
    ): BoxPlot = BoxPlot(lower, middle, upper, x, ymin, ymax, outlierSize)

    def ticker(
        style: String,
        widthOpt: Option[ScalExpr],
        ts: ScalExpr,
        open: ScalExpr,
        high: ScalExpr,
        low: ScalExpr,
        close: ScalExpr
    ): Ticker = Ticker(style, widthOpt, ts, open, high, low, close)

    def geoMap(
        mapId: ScalExpr
    ): GeoMap = GeoMap(mapId)

    def text(
        label: ScalExpr,
        x: ScalExpr,
        y: ScalExpr
    ): Text = Text(label, x, y)

    def apply(geomType: String, params: List[(String, ScalExpr)]): Geom =
        apply(geomType, Map() ++ params)

    def apply(
        geomType: String,
        params: Map[String, ScalExpr]
    ): Geom = {
        def param(s: String): ScalExpr = params.get(s) getOrElse ColRef(s)

        geomType.toUpperCase match {
            case ("POINT" | "SCATTER") =>
                point(param("X"), param("Y"))

            case "LINE" =>
                line(param("X"), param("Y"))

            case "POINTRANGEY" =>
                pointRangeY(
                    param("X"), param("Y"), param("YMIN"), param("YMAX")
                )

            case ("ERRORBAR" | "RANGEY") =>
                rangeY(
                    param("X"), param("YMIN"), param("YMAX"),
                    params.get("WIDTH")
                )

            case ("ERRORBARH" | "RANGEX") =>
                rangeX(
                    param("XMIN"), param("XMAX"), param("Y"),
                    params.get("HEIGHT")
                )

            case ("RIBBON") =>
                ribbon(param("X"), param("YMIN"), param("YMAX"))

            case "ABLINE" =>
                abLine(param("YINTERCEPT"), param("SLOPE"))

            case "HLINE" =>
                hLine(param("Y"))

            case "VLINE" =>
                vLine(param("X"))

            case "SEGMENT" =>
                segment(param("X"), param("XEND"), param("Y"), param("YEND"))

            case "ARROW" =>
                arrow(param("X"), param("XEND"), param("Y"), param("YEND"))

            case "AREA" =>
                area(param("X"), param("Y"))

            case "RECT" =>
                rect(param("XMIN"), param("XMAX"), param("YMIN"), param("YMAX"))

            case "HISTOGRAM" | "HIST" =>
                hist(param("XMIN"), param("XMAX"), param("FREQ"))

            case "BAR" =>
                bar(param("X"), param("Y"))

            case "REGIONX" =>
                regionX(
                    param("XMIN"), param("XMAX"),
                    params.get("YMIN"), params.get("YMAX")
                )

            case "REGIONY" =>
                regionY(
                    param("YMIN"), param("YMAX"),
                    params.get("XMIN"), params.get("XMAX")
                )

            case "OHLC" =>
                ticker(
                    "ohlc",
                    params.get("WIDTH"),
                    param("TS"),
                    param("OPEN"), param("HIGH"), param("LOW"), param("CLOSE")
                )

            case ("TICKER" | "CANDLESTICK") =>
                ticker(
                    "candlestick",
                    params.get("WIDTH"),
                    param("TS"),
                    param("OPEN"), param("HIGH"), param("LOW"), param("CLOSE")
                )

            case _ =>
                throw new IllegalArgumentException(
                    "Invalid geometry specification: " + geomType
                )
        }
    }
}

case object Blank extends Geom {
    override val geomType: String = "blank"

    override val defaultAes: Map[String, AesElement] = Map()

    override val propMap: Map[String, ScalExpr] = Map()

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = None

    override def xAxisExprOpt: Option[ScalExpr] = None
    override def yAxisExprOpt: Option[ScalExpr] = None
}

case class Point(
    x: ScalExpr,
    y: ScalExpr
) extends Geom {
    override val geomType: String = "point"

    override val defaultAes: Map[String, AesElement] = Map(
        "size" -> AesElement.aesConst(DoubleConst(64)),
        "shape" -> AesElement.aesConst(CharConst("circle")),
        "fill" -> AesElement.aesConst(CharConst("black")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("none")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px"))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "x" -> x,
        "y" -> y
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodge(
                key = List("x"),
                order,
                params
            )

        case ("STACK", params) if params.isEmpty =>
            Position.stack(
                key = List("x"),
                stackables = List("y"),
                order,
                params
            )

        case ("JITTER", params) =>
            Position.jitter(
                key = List("x", "y"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = Some(y)
}

case class Line(
    x: ScalExpr,
    y: ScalExpr,
    isClosed: Boolean,
    isArea: Boolean
) extends Geom {
    override val geomType: String = "line"

    override val defaultAes: Map[String, AesElement] = {
        val fill: String = if( isArea ) "steelblue" else "none"
        val stroke: String = if( isArea ) "none" else "black"
        Map(
            "fill" -> AesElement.aesConst(CharConst(fill)),
            "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
            "stroke" -> AesElement.aesConst(CharConst(stroke)),
            "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
            "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
            "interpolate" -> AesElement.aesConst(CharConst("linear")),
            "tension" -> AesElement.aesConst(DoubleConst(0.7))
        )
    }

    override val propMap: Map[String, ScalExpr] = Map(
        "x" -> x,
        "y" -> y,
        "isclosed" -> BoolConst(isClosed),
        "isarea" -> BoolConst(isArea)
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("STACK", params) if params.isEmpty =>
            Position.stack(
                key = List("x"),
                stackables = List("y"),
                order,
                params
            )

        case ("JITTER", params) =>
            Position.jitter(
                key = List("x", "y"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = Some(y)
}

case class PointRangeY(
    x: ScalExpr,
    y: ScalExpr,
    ymin: ScalExpr,
    ymax: ScalExpr
) extends Geom {
    override val geomType: String = "pointrangey"

    override val defaultAes: Map[String, AesElement] = Map(
        "size" -> AesElement.aesConst(DoubleConst(64)),
        "shape" -> AesElement.aesConst(CharConst("circle")),
        "fill" -> AesElement.aesConst(CharConst("none")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "x" -> x,
        "y" -> y,
        "ymin" -> ymin,
        "ymax" -> ymax
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodge(
                key = List("x"),
                order,
                params
            )

        case ("STACK", params) if params.isEmpty =>
            Position.stack(
                key = List("x"),
                stackables = List("y"),
                order,
                params
            )

        case ("JITTER", params) =>
            Position.jitter(
                key = List("x", "y"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = Some(y)
}

case class RangeY(
    x: ScalExpr,
    ymin: ScalExpr,
    ymax: ScalExpr,
    widthOpt: Option[ScalExpr]
) extends Geom {
    override val geomType: String = "rangey"

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("none")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "x" -> x,
        "ymin" -> ymin,
        "ymax" -> ymax,
        "width" -> widthOpt.getOrElse(SqlNull())
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodge(
                key = List("x"),
                order,
                params
            )

        case ("JITTER", params) =>
            Position.jitter(
                key = List("x", "ymin"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = Some(ymin)
}

case class RangeX(
    xmin: ScalExpr,
    xmax: ScalExpr,
    y: ScalExpr,
    heightOpt: Option[ScalExpr]
) extends Geom {
    override val geomType: String = "rangex"

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("none")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "xmin" -> xmin,
        "xmax" -> xmax,
        "y" -> y,
        "height" -> heightOpt.getOrElse(SqlNull())
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodge(
                key = List("y"),
                order,
                params
            )

        case ("JITTER", params) =>
            Position.jitter(
                key = List("xmin", "y"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(xmin)
    override def yAxisExprOpt: Option[ScalExpr] = Some(y)
}

case class Ribbon(
    x: ScalExpr,
    ymin: ScalExpr,
    ymax: ScalExpr
) extends Geom {
    override val geomType: String = "ribbon"

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("gray")),
        "alpha" -> AesElement.aesConst(DoubleConst(0.2)),
        "stroke" -> AesElement.aesConst(CharConst("none")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "x" -> x,
        "ymin" -> ymin,
        "ymax" -> ymax
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("JITTER", params) =>
            Position.jitter(
                key = List("x", "ymin"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = Some(ymin)
}

case class ABLine(
    yIntercept: ScalExpr,
    slope: ScalExpr
) extends Geom {
    override val geomType: String = "abline"

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("none")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "yintercept" -> yIntercept,
        "slope" -> slope
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("JITTER", params) =>
            Position.jitter(
                key = List("yintercept", "slope"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = None
    override def yAxisExprOpt: Option[ScalExpr] = Some(yIntercept)
}

case class VLine(
    x: ScalExpr
) extends Geom {
    override val geomType: String = "vline"

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("none")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7))
    )

    override val propMap: Map[String, ScalExpr] = Map("x" -> x)

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodge(
                key = List("x"),
                order,
                params
            )

        case ("JITTER", params) =>
            Position.jitter(
                key = List("x"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = None
}

case class Segment(
    x: ScalExpr,
    xend: ScalExpr,
    y: ScalExpr,
    yend: ScalExpr,
    isArrow: Boolean
) extends Geom {
    override val geomType: String = "segment"

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("none")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "x" -> x,
        "xend" -> xend,
        "y" -> y,
        "yend" -> yend,
        "isarrow" -> BoolConst(isArrow)
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("JITTER", params) =>
            Position.jitter(
                key = List("x", "y"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = Some(y)
}

case class Bar(
    x: ScalExpr,
    y: ScalExpr
) extends Geom {
    override val geomType: String = "bar"

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("steelblue")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("none")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px"))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "x" -> x,
        "y" -> y
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodgeBar(
                key = List("x"),
                order,
                params
            )

        case ("STACK", params) =>
            Position.stackBar(
                key = List("x"),
                stackables = List("y"),
                order,
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = Some(y)
}

case class Rect(
    xmin: ScalExpr,
    xmax: ScalExpr,
    ymin: ScalExpr,
    ymax: ScalExpr
) extends Geom {
    override val geomType: String = "rect"

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("steelblue")),
        "alpha" -> AesElement.aesConst(DoubleConst(0.7)),
        "stroke" -> AesElement.aesConst(CharConst("none")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px"))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "xmin" -> xmin,
        "xmax" -> xmax,
        "ymin" -> ymin,
        "ymax" -> ymax
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = None

    override def xAxisExprOpt: Option[ScalExpr] = Some(xmin)
    override def yAxisExprOpt: Option[ScalExpr] = Some(ymin)
}

sealed abstract class Region extends Geom {
    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> AesElement.aesConst(CharConst("steelblue")),
        "alpha" -> AesElement.aesConst(DoubleConst(0.7)),
        "stroke" -> AesElement.aesConst(CharConst("none")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px"))
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = None
}

sealed abstract class Region1D extends Region {
    val min: ScalExpr
    val max: ScalExpr
    val beginOpt: Option[ScalExpr]
    val endOpt: Option[ScalExpr]

    override val propMap: Map[String, ScalExpr] = Map(
        "min" -> min,
        "max" -> max,
        "begin" -> beginOpt.getOrElse(SqlNull()),
        "end" -> endOpt.getOrElse(SqlNull())
    )
}

case class RegionX(
    override val min: ScalExpr,
    override val max: ScalExpr,
    override val beginOpt: Option[ScalExpr],
    override val endOpt: Option[ScalExpr]
) extends Region1D {
    override val geomType: String = "regionx"

    override def xAxisExprOpt: Option[ScalExpr] = Some(min)
    override def yAxisExprOpt: Option[ScalExpr] = beginOpt orElse endOpt
}

case class RegionY(
    override val min: ScalExpr,
    override val max: ScalExpr,
    override val beginOpt: Option[ScalExpr],
    override val endOpt: Option[ScalExpr]
) extends Region1D {
    override val geomType: String = "regiony"

    override def xAxisExprOpt: Option[ScalExpr] = beginOpt orElse endOpt
    override def yAxisExprOpt: Option[ScalExpr] = Some(min)
}

case class BoxPlot(
    lower: ScalExpr,
    middle: ScalExpr,
    upper: ScalExpr,
    x: ScalExpr,
    ymin: ScalExpr,
    ymax: ScalExpr,
    outlierSize: ScalExpr
) extends Geom {
    override val geomType: String = "boxplot"

    override val defaultAes: Map[String, AesElement] = Map(
        "shape" -> AesElement.aesConst(CharConst("circle")),
        "size" -> AesElement.aesConst(DoubleConst(64)),
        "fill" -> AesElement.aesConst(CharConst("none")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "lower" -> lower,
        "middle" -> middle,
        "upper" -> upper,
        "x" -> x,
        "ymin" -> ymin,
        "ymax" -> ymax,
        "outlierSize" -> outlierSize
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodge(
                key = List("x"),
                order,
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(x)
    override def yAxisExprOpt: Option[ScalExpr] = Some(ymin)
}

case class Ticker(
    style: String,
    widthOpt: Option[ScalExpr],
    ts: ScalExpr,
    open: ScalExpr,
    high: ScalExpr,
    low: ScalExpr,
    close: ScalExpr
) extends Geom {
    override val geomType: String = "ticker"

    private val color: AesExpr = AesElement.aesExpr(
        CaseExpr(
            ScalOpExpr(LessThan, List(open, close)),
            List((BoolConst(true), CharConst("green"))),
            CharConst("red")
        ),
        IdentityScaleOrdinal
    )

    override val defaultAes: Map[String, AesElement] = Map(
        "fill" -> {
            if( style == "ohlc" ) AesElement.aesConst(CharConst("none"))
            else color
        },
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> color,
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px"))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "style" -> CharConst(style),
        "width" -> widthOpt.getOrElse(SqlNull()),
        "ts" -> ts,
        "open" -> open,
        "high" -> high,
        "low" -> low,
        "close" -> close
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodge(
                key = List("ts"),
                order,
                params
            )

        case ("JITTER", params) =>
            Position.jitter(
                key = List("ts", "open"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = Some(ts)
    override def yAxisExprOpt: Option[ScalExpr] = Some(close)
}

case class GeoMap(
    mapId: ScalExpr
) extends Geom {
    override val geomType: String = "map"

    override val defaultAes: Map[String, AesElement] = Map()

    override val propMap: Map[String, ScalExpr] = Map(
        "mapid" -> mapId
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = None

    override def xAxisExprOpt: Option[ScalExpr] = None
    override def yAxisExprOpt: Option[ScalExpr] = None
}

case class Text(
    label: ScalExpr,
    x: ScalExpr,
    y: ScalExpr
) extends Geom {
    override val geomType: String = "text"

    override val defaultAes: Map[String, AesElement] = Map(
        "size" -> AesElement.aesConst(DoubleConst(64)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "hjust" -> AesElement.aesConst(DoubleConst(0.5)),
        "vjust" -> AesElement.aesConst(DoubleConst(0.5))
    )

    override val propMap: Map[String, ScalExpr] = Map(
        "label" -> label,
        "x" -> x,
        "y" -> y
    )

    override def positionOpt(
        spec: (String, Map[String, Double]),
        order: List[SortExpr] = Nil
    ): Option[Position] = Some(spec).map {
        case ("DODGE", params) =>
            Position.dodge(
                key = List("x"),
                order,
                params
            )

        case ("STACK", params) if params.isEmpty =>
            Position.stack(
                key = List("x"),
                stackables = List("y"),
                order,
                params
            )

        case ("JITTER", params) =>
            Position.jitter(
                key = List("x", "y"),
                params
            )

        case (s, _) =>
            throw new IllegalArgumentException(
                "Invalid position specification for " + geomType + ": " + s
            )
    }

    override def xAxisExprOpt: Option[ScalExpr] = None
    override def yAxisExprOpt: Option[ScalExpr] = None
}
