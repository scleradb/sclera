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

/** Axis specification */
case class Axis(
    labelOpt: Option[String],
    orient: AxisOrient,
    scale: Scale,
    isFree: Boolean,
    isIncreasing: Boolean,
    isZoomIn: Boolean,
    windowSizeOpt: Option[Int],
    tickFormatOpt: Option[String],
    ticksOpt: Option[Int],
    weightOpt: Option[Double]
)

object Axis {
    lazy val xDefault: Axis = default(AxisOrient.bottom)
    lazy val yDefault: Axis = default(AxisOrient.left)

    def default(orient: AxisOrient): Axis = Axis(
        labelOpt = None,
        orient,
        Scale.linear,
        isFree = false,
        isIncreasing = false,
        isZoomIn = true,
        windowSizeOpt = None,
        tickFormatOpt = None,
        ticksOpt = None,
        weightOpt = None
    )

    def default(axisId: AxisId): Axis = axisId match {
        case AxisX => xDefault
        case AxisY => yDefault
    }
}
