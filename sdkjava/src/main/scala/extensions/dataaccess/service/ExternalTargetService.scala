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

package com.scleradb.java.external.service

import com.scleradb.sql.expr.ScalValueBase
import com.scleradb.external.objects.ExternalTarget
import com.scleradb.external.service.{ExternalTargetService => ScalaExtService}

/** Abstract base class for all external data services - Java version */
abstract class ExternalTargetService extends ScalaExtService {
    /** Create a target object using the given generic parameters */
    def createTarget(
        params: Array[ScalValueBase]
    ): ExternalTarget = throw new IllegalArgumentException(
        "Cannot write to service: " + id
    )

    /** Create a target object using the given generic parameters (Scala) */
    override def createTarget(
        params: List[ScalValueBase]
    ): ExternalTarget = createTarget(params.toArray)
}
