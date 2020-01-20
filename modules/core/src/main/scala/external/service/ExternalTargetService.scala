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

package com.scleradb.external.service

import com.scleradb.service.{ScleraService, ScleraServiceLoader}

import com.scleradb.sql.expr.{ScalValueBase, SortExpr}
import com.scleradb.external.objects.{ExternalSource, ExternalTarget}

/** Abstract base class for all external data target services */
abstract class ExternalTargetService extends ScleraService {
    /** Create a target object using the given generic parameters
      * Default throws as exception
      * @param params Generic parameters
      */
    def createTarget(
        params: List[ScalValueBase]
    ): ExternalTarget = throw new IllegalArgumentException(
        "Cannot write to service: " + id
    )
}

/** Companion object - helps create an external data target service */
private[scleradb]
object ExternalTargetService extends ScleraServiceLoader(
    classOf[ExternalTargetService]
)
