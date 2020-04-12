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

import com.scleradb.sql.types.SqlType
import com.scleradb.sql.expr.ScalColValue

import com.scleradb.external.service.ExternalFunctionService

/** Abstract base class for all external functions */
abstract class ExternalFunction extends java.io.Serializable {
    /** Name of the function*/
    val name: String
    /** Type of the result computed by the function */
    val resultType: SqlType
    /** Compute the function result */
    def result(args: List[ScalColValue]): ScalColValue
}

/** Companion object - helps create external function objects */
object ExternalFunction {
    /** Get an external functuon object from a function service
      * @param name External function service identifier
      * @param functionName External function name
      * @return External function object
      */
    def apply(
        name: String,
        functionName: String
    ): ExternalFunction =
        ExternalFunctionService(name).createFunction(functionName)
}
