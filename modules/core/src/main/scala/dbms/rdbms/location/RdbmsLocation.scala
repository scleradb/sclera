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

package com.scleradb.dbms.rdbms.location

import com.scleradb.dbms.location.Location
import com.scleradb.sql.mapper.SqlMapper

import com.scleradb.dbms.rdbms.driver.SqlDriver

abstract class RdbmsLocation extends Location {
    /** Database name */
    val dbName: String

    /** Internal representation -> SQL mapper */
    val sqlMapper: SqlMapper
    /** JDBC base url */
    val url: String

    /** Driver for SQL/JDBC interface */
    override def driver: SqlDriver

    override def supportedFunctionsOpt: Option[List[String]] =
        sqlMapper.functionMapOpt.map { m => m.keys.toList }
}
