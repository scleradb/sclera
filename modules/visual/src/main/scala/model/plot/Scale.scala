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

import com.scleradb.sql.expr.{ScalValueBase, NumericConst}

/** Scale */
sealed abstract class Scale

object Scale {
    def identity(scale: Scale): IdentityScale = scale match {
        case (_: OrdinalScale) => IdentityScaleOrdinal
        case (_: QuantScale) => IdentityScaleQuant
        case (ident: IdentityScale) => ident
    }

    lazy val time: TimeScale = time((None, None), None)
    def time(
        domain: (Option[ScalValueBase], Option[ScalValueBase]),
        rangeOpt: Option[(Double, Double)]
    ): TimeScale = TimeScale(domain, rangeOpt)

    lazy val linear: LinearScale = linear((None, None), None)
    def linear(range: (Double, Double)): LinearScale =
        linear((None, None), Some(range))
    def linear(
        domain: (Option[ScalValueBase], Option[ScalValueBase]),
        rangeOpt: Option[(Double, Double)]
    ): LinearScale = LinearScale(domain, rangeOpt)

    lazy val log: LogScale = log((None, None), None)
    def log(
        domain: (Option[ScalValueBase], Option[ScalValueBase]),
        rangeOpt: Option[(Double, Double)]
    ): LogScale = LogScale(domain, rangeOpt)

    lazy val sqrt: SqrtScale = sqrt((None, None), None)
    def sqrt(
        domain: (Option[ScalValueBase], Option[ScalValueBase]),
        rangeOpt: Option[(Double, Double)]
    ): SqrtScale = SqrtScale(domain, rangeOpt)

    def quant(
        name: String,
        domainOpt: Option[(Option[ScalValueBase], Option[ScalValueBase])],
        optRangeOpt: Option[(Option[ScalValueBase], Option[ScalValueBase])]
    ): QuantScale = {
        val domain: (Option[ScalValueBase], Option[ScalValueBase]) =
            domainOpt match {
                case None => (None, None)
                case Some(domain) => domain
            }

        val rangeOpt: Option[(Double, Double)] =
            optRangeOpt match {
                case None => None
                case Some((Some(a: NumericConst), Some(b: NumericConst))) =>
                        Some((a.numericValue, b.numericValue))
                case _ => throw new IllegalArgumentException(
                    "Invalid range specified for \"" + name + "\""
                )
            }

        name match {
            case "TIME" => time(domain, rangeOpt)
            case "LINEAR" => linear(domain, rangeOpt)
            case "LOG" => log(domain, rangeOpt)
            case "SQRT" => sqrt(domain, rangeOpt)
            case s =>
                throw new IllegalArgumentException(
                    "Invalid scale specification: " + s
                )
        }
    }

    lazy val ordinal: OrdinalValueScale = ordinal(None, Nil, false)
    lazy val band: OrdinalValueScale = ordinal(None, Nil, true)
    def ordinal(
        domainOpt: Option[List[ScalValueBase]],
        range: List[ScalValueBase],
        isBand: Boolean = false
    ): OrdinalValueScale = OrdinalValueScale(domainOpt, range, isBand)

    lazy val color: ColorScale = color(None, Nil)

    def color(scheme: String): ColorScale = color(None, List(scheme))

    def color(
        domainOpt: Option[List[ScalValueBase]],
        range: List[String]
    ): ColorScale = ColorScale(domainOpt, range)

    lazy val symbol: SymbolScale = symbol(None, Nil)
    def symbol(
        domainOpt: Option[List[ScalValueBase]],
        range: List[String]
    ): SymbolScale = SymbolScale(domainOpt, range)

    def default(aesProp: String): Scale = aesProp.toLowerCase match {
        case "size" => linear(range = (0, 1))
        case "shape" => symbol
        case "fill" => color
        case "alpha" => linear(range = (0, 1))
        case "stroke" => color
        case "stroke-dasharray" => linear(range = (5, 15))
        case "stroke-width" => linear(range = (0.5, 2.5))
        case "hjust" => linear(range = (0.5, 1))
        case "vjust" => linear(range = (0.5, 1))
        case s =>
            throw new IllegalArgumentException(
                "Invalid aesthetics specification: " + s
            )
    }
}

/** Quant base scale */
sealed abstract class QuantScale extends Scale {
    val domain: (Option[ScalValueBase], Option[ScalValueBase])
    val rangeOpt: Option[(Double, Double)]
}

/** Time scale */
case class TimeScale(
    override val domain: (Option[ScalValueBase], Option[ScalValueBase]),
    override val rangeOpt: Option[(Double, Double)]
) extends QuantScale

/** Linear scale */
case class LinearScale(
    override val domain: (Option[ScalValueBase], Option[ScalValueBase]),
    override val rangeOpt: Option[(Double, Double)]
) extends QuantScale

/** Log scale */
case class LogScale(
    override val domain: (Option[ScalValueBase], Option[ScalValueBase]),
    override val rangeOpt: Option[(Double, Double)]
) extends QuantScale

/** Sqrt scale */
case class SqrtScale(
    override val domain: (Option[ScalValueBase], Option[ScalValueBase]),
    override val rangeOpt: Option[(Double, Double)]
) extends QuantScale

/** Ordinal base scale */
sealed abstract class OrdinalScale extends Scale {
    val domainOpt: Option[List[ScalValueBase]]
}

/** Ordinal scale */
case class OrdinalValueScale(
    override val domainOpt: Option[List[ScalValueBase]],
    range: List[ScalValueBase],
    isBand: Boolean
) extends OrdinalScale

sealed abstract class AesOrdinalScale extends OrdinalScale {
    val range: List[String]
}

/** Color scale */
case class ColorScale(
    override val domainOpt: Option[List[ScalValueBase]],
    override val range: List[String]
) extends AesOrdinalScale

/** Symbol scale */
case class SymbolScale(
    override val domainOpt: Option[List[ScalValueBase]],
    override val range: List[String]
) extends AesOrdinalScale

/** Identity scale */
sealed abstract class IdentityScale extends Scale

case object IdentityScaleOrdinal extends IdentityScale
case object IdentityScaleQuant extends IdentityScale
