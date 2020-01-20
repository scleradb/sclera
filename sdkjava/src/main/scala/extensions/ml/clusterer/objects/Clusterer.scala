/**
* Sclera Extensions - Java SDK
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

package com.scleradb.java.analytics.ml.clusterer.objects

import com.scleradb.sql.result.TableRow

import com.scleradb.analytics.ml.datatypes.DataAttribute
import com.scleradb.analytics.ml.clusterer.objects.{Clusterer => SClusterer}

/** Abstract base class for clusterer libraries */
abstract class Clusterer extends SClusterer {
    /** Clusterer name */
    override val name: String

    /** Feature attributes */
    def attrsArray: Array[DataAttribute]

    /** Feature attributes (Scala) */
    override lazy val attrs: List[DataAttribute] = attrsArray.toList

    /** Assign cluster to a row
      * @param t Table row to be assigned a cluster;
      *        must contain the feature attribute columns
      * @return Identifier of the assigned cluster
      */
    override def cluster(t: TableRow): Int

    /** Clusterer description */
    override def description: String
}
