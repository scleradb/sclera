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

import java.io.File

import com.scleradb.exec.Schema
import com.scleradb.config.ScleraConfig
import com.scleradb.sql.mapper.SqlMapper

import com.scleradb.dbms.location.{LocationId, LocationPermit}
import com.scleradb.dbms.rdbms.location.RdbmsLocation
import com.scleradb.dbms.rdbms.driver.SqlDriver
import com.scleradb.dbms.rdbms.h2.driver.H2SqlDriver
import com.scleradb.dbms.rdbms.h2.mapper.H2SqlMapper

private[scleradb]
class H2(
    override val schema: Schema,
    override val id: LocationId,
    override val dbName: String,
    override val dbSchemaOpt: Option[String],
    override val config: List[(String, String)],
    override val permit: LocationPermit
) extends RdbmsLocation {
    private val jdbcDriverClass: Class[org.h2.Driver] =
        classOf[org.h2.Driver]

    override val isTemporary: Boolean = false
    override val dbms: String = H2.id
    override val param: String = dbName

    override val sqlMapper: SqlMapper = new H2SqlMapper(this)

    private val h2Dir: File = new File(ScleraConfig.dataDir, "H2")
    h2Dir.mkdirs()

    private val dataDir: File = new File(h2Dir, dbName)
    override val url: String =
        "jdbc:h2:" + dataDir.getAbsolutePath() +
        ";IGNORECASE=TRUE" // ;MODE=PostgreSQL"

    override def driver: SqlDriver =
        new H2SqlDriver(this, sqlMapper, url, config)
}

private[scleradb]
object H2 {
    val id: String = "H2"
}
