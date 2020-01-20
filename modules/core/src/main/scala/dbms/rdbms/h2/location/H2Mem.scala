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

package com.scleradb.dbms.rdbms.h2.location

import com.scleradb.exec.Schema
import com.scleradb.sql.mapper.SqlMapper

import com.scleradb.dbms.location.{LocationId, LocationPermit}
import com.scleradb.dbms.rdbms.location.RdbmsLocation
import com.scleradb.dbms.rdbms.driver.SqlDriver
import com.scleradb.dbms.rdbms.h2.driver.H2SqlDriver
import com.scleradb.dbms.rdbms.h2.mapper.H2SqlMapper

private[scleradb]
class H2Mem(
    override val schema: Schema,
    override val id: LocationId,
    override val dbName: String,
    override val dbSchemaOpt: Option[String],
    override val config: List[(String, String)],
    override val permit: LocationPermit
) extends RdbmsLocation {
    private val jdbcDriverClass: Class[org.h2.Driver] =
        classOf[org.h2.Driver]

    override val isTemporary: Boolean = true
    override val dbms: String = H2Mem.id
    override val param: String = dbName

    override val sqlMapper: SqlMapper = new H2SqlMapper(this)

    override val url: String =
        "jdbc:h2:mem:" + dbName + ";IGNORECASE=TRUE" // ;MODE=PostgreSQL"

    override def driver: SqlDriver =
        new H2SqlDriver(this, sqlMapper, url, config)
}

private[scleradb]
object H2Mem {
    val id: String = "H2MEM"
}
