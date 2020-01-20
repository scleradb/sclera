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

/** Display dimensions */
case class Display(
    width: Double,
    height: Double,
    margin: DisplayMargin,
    legendDisplay: LegendDisplay
)

/** Display margin */
case class DisplayMargin(
    top: Double,
    right: Double,
    bottom: Double,
    left: Double
)

/** Legend dimensions */
case class LegendDisplay(
    padding: Double,
    width: Double
)

object Display {
    lazy val default: Display = Display(
        width = 800,
        height = 450,
        margin = DisplayMargin(top = 20, right = 20, bottom = 30, left = 40),
        legendDisplay = LegendDisplay(padding = 20, width = 100)
    )
}
