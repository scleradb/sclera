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

package com.scleradb.visual.model.spec

import com.scleradb.sql.expr.{ScalValueBase, NumericConst}

sealed abstract class ScaleSpec

case class QuantScaleSpec(
    name: String,
    domain: (Option[ScalValueBase], Option[ScalValueBase]),
    rangeOpt: Option[(Double, Double)]
) extends ScaleSpec

case class OrdinalScaleSpec(
    domainOpt: Option[List[ScalValueBase]],
    range: List[ScalValueBase]
) extends ScaleSpec

case class ColorScaleSpec(
    domainOpt: Option[List[ScalValueBase]],
    range: List[String]
) extends ScaleSpec

case class SymbolScaleSpec(
    domainOpt: Option[List[ScalValueBase]],
    range: List[String]
) extends ScaleSpec

object ScaleSpec {
    def quant(
        name: String,
        domainOpt: Option[(Option[ScalValueBase], Option[ScalValueBase])],
        optRangeOpt: Option[(Option[ScalValueBase], Option[ScalValueBase])]
    ): QuantScaleSpec = {
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

        QuantScaleSpec(name, domain, rangeOpt)
    }

    def ordinal(
        domainOpt: Option[List[ScalValueBase]],
        range: List[ScalValueBase]
    ): OrdinalScaleSpec =
        OrdinalScaleSpec(domainOpt, range)

    def color(
        domainOpt: Option[List[ScalValueBase]],
        range: List[String]
    ): ColorScaleSpec =
        ColorScaleSpec(domainOpt, range)

    def symbol(
        domainOpt: Option[List[ScalValueBase]],
        range: List[String]
    ): SymbolScaleSpec =
        SymbolScaleSpec(domainOpt, range)
}
