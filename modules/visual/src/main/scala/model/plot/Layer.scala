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

import com.scleradb.sql.expr.{ScalExpr, SortExpr}
import com.scleradb.visual.model.spec.LayerSetTask

/** Layer */
case class Layer(
    geom: Geom,
    aes: Aes,
    groupOpt: Option[ScalExpr],
    keyOpt: Option[ScalExpr],
    posOpt: Option[Position],
    stats: List[(Stat, List[LayerSetTask])], // tasks act on the output of stat
    marks: List[(Mark, Option[AxisId])],
    tooltipOpt: Option[ScalExpr],
    isHidden: Boolean,
    isGenerated: Boolean,
    displayOrder: Double
) {
    // stat tasks ignored because they apply after the stat rewrite
    def scalExprs: List[ScalExpr] =
        geom.scalExprs ::: aes.scalExprs :::
        groupOpt.toList :::keyOpt.toList :::
        tooltipOpt.toList ::: posOpt.toList.flatten(pos => pos.scalExprs) :::
        stats.flatMap { case (stat, _) => stat.scalExprs } :::
        marks.flatMap { case (mark, _) => mark.scalExprs }
}

object Layer {
    lazy val default: Layer = Layer(
        Geom.default, Aes.default(Geom.default),
        groupOpt = None, keyOpt = None,
        posOpt = None, stats = Nil, marks = Nil,
        tooltipOpt = None, isHidden = false,
        isGenerated = false, displayOrder = 0
    )
}
