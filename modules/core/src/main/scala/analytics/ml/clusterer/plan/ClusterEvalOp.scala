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

package com.scleradb.analytics.ml.clusterer.plan

import com.scleradb.exec.Schema

import com.scleradb.sql.expr.SortExpr
import com.scleradb.sql.result.TableResult
import com.scleradb.sql.plan.{RelEvalOp, RelEvalPlanResult}

import com.scleradb.analytics.ml.clusterer.expr.Cluster

case class ClusterEvalOp(
    schema: Schema,
    clusterOp: Cluster
) extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val inputResult: TableResult = inputResults.head.tableResult
        val result: TableResult =
            clusterOp.clusterer(schema).cluster(
                inputResult, clusterOp.clusterIdColRef
            )

        assert(SortExpr.isSubsumedBy(resOrder, result.resultOrder))

        RelEvalPlanResult(result)
    }
}
