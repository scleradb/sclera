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

package com.scleradb.sql.plan

import com.scleradb.sql.expr.RelExpr
import com.scleradb.sql.result.TableResult

import com.scleradb.plan.PlanResult

private[scleradb]
sealed abstract class RelPlanResult extends PlanResult

private[scleradb]
case class RelExprPlanResult(relExpr: RelExpr) extends RelPlanResult

private[scleradb]
case class RelEvalPlanResult(tableResult: TableResult) extends RelPlanResult {
    def close(): Unit = tableResult.close()
}
