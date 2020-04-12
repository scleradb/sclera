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

package com.scleradb.external.objects

import com.scleradb.sql.expr.{SortExpr, ScalValueBase}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableResult

import com.scleradb.external.service.ExternalSourceService

/** Abstract base class for all external data sources */
abstract class ExternalSource extends java.io.Serializable {
    /** Name of the data source */
    val name: String
    /** Columns of the rows emitted by the data source */
    val columns: List[Column]
    /** The rows emitted by the data source */
    def result: TableResult
}

/** Companion object - helps create external data source objects */
object ExternalSource {
    /** Get an external data source object from a data service
      * @param name External data service identifier
      * @param params Generic parameters for data source object creation
      * @return Data source object
      */
    def apply(
        name: String,
        params: List[ScalValueBase]
    ): ExternalSource = ExternalSourceService(name).createSource(params)
}
