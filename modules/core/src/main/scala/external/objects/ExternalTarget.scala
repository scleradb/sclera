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

import com.scleradb.sql.expr.ScalValueBase
import com.scleradb.sql.result.TableResult

import com.scleradb.external.service.ExternalTargetService

/** Abstract base class for all external data targets */
abstract class ExternalTarget extends java.io.Serializable {
    /** Name of the data target */
    val name: String
    /** Write the data in the table result
      * @param ts Table result containg the data to be writted to the target
      */
    def write(ts: TableResult): Unit
}

/** Companion object - helps create external data target objects */
private[scleradb]
object ExternalTarget {
    /** Get an external data target object from a data service
      * @param name External data service identifier
      * @param params Generic parameters for data target object creation
      * @return Data target object
      */
    def apply(
        name: String,
        params: List[ScalValueBase]
    ): ExternalTarget =
        ExternalTargetService(name).createTarget(params)
}
