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

package com.scleradb.analytics.transform.objects

import com.scleradb.sql.types.SqlType
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.expr.{ScalExpr, ScalColValue}

abstract class Transformer {
    def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType]

    def transform(
        inputColTypes: Map[String, SqlType],
        inputVals: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]]
}
