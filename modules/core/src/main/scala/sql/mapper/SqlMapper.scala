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

package com.scleradb.sql.mapper

import com.scleradb.sql.statements._

/** SQL Mapper base class - used to generate SQL from internal representation */
abstract class SqlMapper {
    /** Generate SQL query string */
    def queryString(query: SqlRelQueryStatement): String
    /** Generate SQL update string */
    def updateString(stmt: SqlUpdateStatement): List[String]

    /** Optional scalar function alias map */
    val functionMapOpt: Option[Map[String, String]] = None

    /** Function alias -- no-op if not specified in the function alias map */
    def functionAlias(name: String): String = functionMapOpt match {
        case Some(m) => m.get(name) getOrElse name
        case None => name
    }
}
