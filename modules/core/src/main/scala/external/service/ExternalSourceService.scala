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
import com.scleradb.external.objects.ExternalSource

/** Abstract base class for all external data source services */
abstract class ExternalSourceService extends ScleraService {
    /** Create a source object using the given generic parameters
      * @param params Generic parameters
      */
    def createSource(params: List[ScalValueBase]): ExternalSource
}

/** Companion object - helps create an external data source service */
object ExternalSourceService extends ScleraServiceLoader(
    classOf[ExternalSourceService]
)
