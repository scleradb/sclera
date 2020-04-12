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

package com.scleradb.dbms.rdbms.h2.driver

import com.scleradb.dbms.location.Location
import com.scleradb.sql.mapper.SqlMapper
import com.scleradb.dbms.rdbms.driver.StdSqlDriver

class H2SqlDriver(
    override val location: Location,
    override val sqlMapper: SqlMapper,
    override val jdbcUrl: String,
    override val configProps: List[(String, String)]
) extends StdSqlDriver {
    override val metadataDriver: H2SqlMetadataDriver =
        new H2SqlMetadataDriver(this, conn.getMetaData())
}
