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

package com.scleradb.visual.exec

import scala.language.postfixOps

import com.scleradb.sql.expr.{ScalExpr, SortExpr, SortAsc}
import com.scleradb.sql.types._
import com.scleradb.sql.exec.ScalTypeEvaluator
import com.scleradb.sql.datatypes.Column

import com.scleradb.visual.model.plot.Scale
import com.scleradb.visual.model.spec._

object PlotInference {
    def axisSetTasks(
        e: ScalExpr,
        cols: List[Column],
        resultOrder: List[SortExpr]
    ): List[AxisSetTask] = List(
        inferScaleOpt(e, cols).map { scale => AxisSetScale(scale) },
        Some(AxisSetLabel(e.repr)),
        Some(AxisSetIncreasing(isIncreasing(e, resultOrder)))
    ) flatten

    private def inferScaleOpt(e: ScalExpr, cols: List[Column]): Option[Scale] =
        inferScaleOpt(ScalTypeEvaluator.eval(e, cols))

    private def inferScaleOpt(sqlType: SqlType): Option[Scale] = sqlType match {
        case SqlOption(t) => inferScaleOpt(t)
        case (_: SqlNumeric) => Some(Scale.linear)
        case SqlBool | (_: SqlChar) => Some(Scale.ordinal)
        case (_: SqlDateTime) => Some(Scale.time)
        case _ => None
    }

    private def isIncreasing(
        e: ScalExpr,
        resultOrder: List[SortExpr]
    ): Boolean = resultOrder.headOption match {
        case Some(SortExpr(se, SortAsc, _)) => (se == e)
        case other => false
    }
}
