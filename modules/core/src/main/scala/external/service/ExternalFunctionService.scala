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
import com.scleradb.external.objects.ExternalFunction

/** Abstract base class for all external function services */
abstract class ExternalFunctionService extends ScleraService {
    /** Create a function object
      * @param funcName Function name
      */
    def createFunction(
        funcName: String
    ): ExternalFunction
}

/** Companion object - helps create an external function service */
private[scleradb]
object ExternalFunctionService extends ScleraServiceLoader(
    classOf[ExternalFunctionService]
)
