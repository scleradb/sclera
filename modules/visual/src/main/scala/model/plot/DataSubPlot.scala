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

import com.scleradb.sql.expr.ScalExpr

/** Data sub-plot */
case class DataSubPlot(layers: List[Layer]) {
    def scalExprs: List[ScalExpr] = layers.flatMap { layer => layer.scalExprs }

    private def axisExprOpt(axisId: AxisId): Option[ScalExpr] =
        layers.filter(layer => !layer.isGenerated).flatMap(
            layer => layer.geom.axisExprOpt(axisId)
        ).headOption.orElse {
            layers.flatMap(layer => layer.geom.axisExprOpt(axisId)).headOption
        }
    
    lazy val xAxisExpr: ScalExpr = axisExprOpt(AxisX) getOrElse {
        throw new IllegalArgumentException(
            "Cannot determine the X axis expression"
        )
    }

    lazy val yAxisExpr: ScalExpr = axisExprOpt(AxisY) getOrElse {
        throw new IllegalArgumentException(
            "Cannot determine the Y axis expression"
        )
    }

    def axisExpr(axisId: AxisId): ScalExpr = axisId match {
        case AxisX => xAxisExpr
        case AxisY => yAxisExpr
    }

    def hasBar(axisId: AxisId): Boolean =  axisId match {
        case AxisX => layers.exists(_.geom.geomType == "bar")
        case AxisY => false
    }
}

object DataSubPlot {
    lazy val filler: DataSubPlot = DataSubPlot(List(Layer.default))
}
