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

package com.scleradb.analytics.ml.clusterer.objects

import com.scleradb.exec.Schema

import com.scleradb.sql.result.{TableResult, TableRow}
import com.scleradb.sql.expr.ColRef

import com.scleradb.analytics.ml.datatypes._
import com.scleradb.analytics.ml.service.MLService
import com.scleradb.analytics.ml.objects.{MLObjectId, MLObject, SchemaMLObject}

import com.scleradb.analytics.ml.clusterer.datatypes.ClusterResult

/** Abstract base class for clusterer libraries */
abstract class Clusterer extends MLObject {
    private[scleradb]
    override def typeStr: String = Clusterer.typeStr

    /** Clusterer name */
    override val name: String
    /** Feature attributes */
    val attrs: List[DataAttribute]

    /** Assign cluster to a row
      * @param t Table row to be assigned a cluster;
      *        must contain the feature attribute columns
      * @return Identifier of the assigned cluster
      */
    def cluster(t: TableRow): Int

    /** Assign clusters to a result set of rows
      * @param rs Table result containing the rows to be assigned a cluster;
      *           schema must contain the feature attribute columns
      * @return A modified tableresult, where each row has an additional column
      *         `clusterIdColRef` containing the identifier of
      *         the row's assigned cluster
      */
    def cluster(rs: TableResult, clusterIdColRef: ColRef): TableResult =
        new ClusterResult(rs, clusterIdColRef, this)

    /** Clusterer description */
    override def description: String

    private[scleradb]
    override def schemaObject: SchemaClusterer = SchemaClusterer(this)
}

private[scleradb]
object Clusterer {
    val typeStr: String = "CLUSTERER"

    def apply(
        serviceIdOpt: Option[String],
        name: String,
        specOpt: Option[(String, String)],
        numDistinctValuesMap: Map[ColRef, Int],
        tableResult: TableResult
    ): Clusterer =
        MLService(serviceIdOpt).createClusterer(
            name, specOpt, numDistinctValuesMap, tableResult
        )
}

private[scleradb]
class SchemaClusterer(override val obj: Clusterer) extends SchemaMLObject

private[scleradb]
object SchemaClusterer {
    def apply(clusterer: Clusterer): SchemaClusterer =
        new SchemaClusterer(clusterer)

    def objectOpt(schema: Schema, id: MLObjectId): Option[SchemaClusterer] =
        objectOpt(schema, id.repr)

    def objectOpt(schema: Schema, id: String): Option[SchemaClusterer] =
        SchemaMLObject.objectOpt(schema, id) match {
            case Some(obj: SchemaClusterer) => Some(obj)
            case _ => None
        }

    def objects(schema: Schema): List[SchemaClusterer] =
        SchemaMLObject.objects(schema).flatMap {
            case (obj: SchemaClusterer) => Some(obj)
            case _ => None
        }
}
