/**
* Sclera - Core
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

package com.scleradb.external.plan

import com.scleradb.sql.result.TableResult
import com.scleradb.sql.plan.{RelEvalPlan, RelEvalPlanResult}

import com.scleradb.external.expr.ExternalSourceExpr

private[scleradb]
case class ExternalSourceEvalPlan(
    override val relExpr: ExternalSourceExpr
) extends RelEvalPlan {
    private var tableResultOpt: Option[TableResult] = None

    override def init(): Unit = { }

    override def result: RelEvalPlanResult = {
        val tableResult: TableResult = relExpr.source.result
        tableResultOpt = Some(tableResult)

        RelEvalPlanResult(tableResult)
    }

    override def dispose(): Unit =
        tableResultOpt.foreach { tableResult => tableResult.close() }
}
