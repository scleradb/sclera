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

import scala.language.postfixOps

import com.scleradb.sql.expr._

case class Aes(
    size: AesElement,
    shape: AesElement,
    fill: AesElement,
    alpha: AesElement,
    stroke: AesElement,
    strokeDashArray: AesElement,
    strokeWidth: AesElement,
    interpolate: AesConst,
    tension: AesConst,
    hjust: AesElement,
    vjust: AesElement
) {
    val propMap: Map[String, AesElement] = Map(
        "size" -> size,
        "shape" -> shape,
        "fill" -> fill,
        "alpha" -> alpha,
        "stroke" -> stroke,
        "stroke-dasharray" -> strokeDashArray,
        "stroke-width" -> strokeWidth,
        "interpolate" -> interpolate,
        "tension" -> tension,
        "hjust" -> hjust,
        "vjust" -> vjust
    )

    def scalExprs: List[ScalExpr] = propMap.values.map { e => e.value } toList
}

object Aes {
    val defaultPropMap: Map[String, AesElement] = Map(
        "shape" -> AesElement.aesConst(CharConst("circle")),
        "size" -> AesElement.aesConst(DoubleConst(64)),
        "fill" -> AesElement.aesConst(CharConst("none")),
        "alpha" -> AesElement.aesConst(DoubleConst(1.0)),
        "stroke" -> AesElement.aesConst(CharConst("black")),
        "stroke-dasharray" -> AesElement.aesConst(CharConst("none")),
        "stroke-width" -> AesElement.aesConst(CharConst("1.5px")),
        "interpolate" -> AesElement.aesConst(CharConst("linear")),
        "tension" -> AesElement.aesConst(DoubleConst(0.7)),
        "hjust" -> AesElement.aesConst(DoubleConst(0.5)),
        "vjust" -> AesElement.aesConst(DoubleConst(0.5))
    )

    def apply(inpMap: Map[String, AesElement] = Map()): Aes = {
        // fill in the missing properties
        val propMap: Map[String, AesElement] = defaultPropMap ++ inpMap

        Aes(
            size = propMap("size"),
            shape = propMap("shape"),
            fill = propMap("fill"),
            alpha = propMap("alpha"),
            stroke = propMap("stroke"),
            strokeDashArray = propMap("stroke-dasharray"),
            strokeWidth = propMap("stroke-width"),
            interpolate = propMap("interpolate").asInstanceOf[AesConst],
            tension = propMap("tension").asInstanceOf[AesConst],
            hjust = propMap("hjust"),
            vjust = propMap("vjust")
        )
    }

    lazy val default: Aes = Aes()
    def default(geom: Geom): Aes = apply(geom.defaultAes)
}

sealed abstract class AesElement {
    val value: ScalExpr
}

object AesElement {
    def aesConst(v: ScalValueBase): AesConst = AesConst(v)

    def aesExpr(
        expr: ScalExpr,
        scale: Scale,
        onNullOpt: Option[ScalValueBase] = None,
        legendOpt: Option[Legend] = None
    ): AesExpr = AesExpr(expr, scale, onNullOpt, legendOpt)
}

case class AesConst(override val value: ScalValueBase) extends AesElement

case class AesExpr(
    override val value: ScalExpr,
    scale: Scale,
    onNullOpt: Option[ScalValueBase],
    legendOpt: Option[Legend]
) extends AesElement
