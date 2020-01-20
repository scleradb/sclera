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
import com.scleradb.visual.model.spec.{AxisSetScale, AxisSetTask}

/** Data plot */
case class DataPlot(
    facetOpt: Option[Facet],
    subPlots: List[DataSubPlot],
    axisTasks: Map[ScalExpr, List[AxisSetTask]]
) {
    def facetExprs: List[ScalExpr] =
        facetOpt.map { facet => facet.scalExprs } getOrElse Nil

    def scalExprs: List[ScalExpr] = facetExprs :::
        subPlots.flatMap { p => p.scalExprs } ::: axisTasks.keys.toList

    def axesSpecs(axisId: AxisId): List[(ScalExpr, List[AxisSetTask])] =
        subPlots.map(p =>
            (p.axisExpr(axisId), p.hasBar(axisId))
        ).distinct.map { case (e, isBar) =>
            val tasks: List[AxisSetTask] = axisTasks.get(e) getOrElse Nil
            if( isBar ) (e, AxisSetScale(Scale.band)::tasks) else (e, tasks)
        }
}

object DataPlot {
    lazy val default: DataPlot =
        DataPlot(facetOpt = None, subPlots = Nil, axisTasks = Map())
}
