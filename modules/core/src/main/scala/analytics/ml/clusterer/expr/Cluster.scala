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

package com.scleradb.analytics.ml.clusterer.expr

import com.scleradb.exec.Schema

import com.scleradb.sql.expr.{RelExpr, ColRef}

import com.scleradb.analytics.ml.objects.MLObjectId
import com.scleradb.analytics.ml.clusterer.objects.{Clusterer, SchemaClusterer}
import com.scleradb.analytics.ml.expr.MLRelOp

case class Cluster(
    clustererId: MLObjectId,
    clusterIdColRef: ColRef
) extends MLRelOp {
    override val arity: Int = 1

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        clusterIdColRef::inputs.head.tableColRefs

    def clusterer(schema: Schema): Clusterer = {
        val schemaClusterer: SchemaClusterer =
            SchemaClusterer.objectOpt(schema, clustererId) getOrElse {
                throw new IllegalArgumentException(
                    "Clusterer \"" + clustererId.repr + "\" not found"
                )
            }

        schemaClusterer.obj
    }
}
