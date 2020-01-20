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

/** Axis orientation */
sealed abstract class AxisOrient

object AxisOrient {
    def left: AxisOrient = AxisLeft
    def right: AxisOrient = AxisRight
    def top: AxisOrient = AxisTop
    def bottom: AxisOrient = AxisBottom

    def apply(s: String): AxisOrient = s.toUpperCase match {
        case "LEFT" => left
        case "RIGHT" => right
        case "TOP" => top
        case "BOTTOM" => bottom
        case _ =>
            throw new IllegalArgumentException(
                "Invalid axis orientation specification: " + s
            )
    }
}

case object AxisLeft extends AxisOrient
case object AxisRight extends AxisOrient
case object AxisTop extends AxisOrient
case object AxisBottom extends AxisOrient
