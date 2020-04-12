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

import java.sql.{DatabaseMetaData, ResultSet}

import com.scleradb.sql.result.TableResult
import com.scleradb.dbms.rdbms.driver.{SqlDriver, SqlMetadataDriver}

class H2SqlMetadataDriver(
    driver: SqlDriver,
    metaData: DatabaseMetaData
) extends SqlMetadataDriver(driver, metaData) {
    override def tablesMetadata(
        catalogOpt: Option[String],
        tableSchemaOpt: Option[String],
        tableNameOpt: Option[String],
        tableTypeStrsOpt: Option[List[String]]
    ): List[(String, String)] = {
        def readMetadata(rs: ResultSet): List[(String, String)] =
            if( rs.next() ) {
                val name: String = rs.getString("TABLE_NAME")
                val tableTypeStr: String = rs.getString("TABLE_TYPE")

                (name, tableTypeStr)::readMetadata(rs)
            } else Nil

        val catalog: String =
            catalogOpt.map { name => name.toUpperCase } getOrElse null
        val tableSchema: String =
            tableSchemaOpt.map { name => name.toUpperCase } getOrElse null
        val tableName: String =
            tableNameOpt.map { name => name.toUpperCase } getOrElse null
        val tableTypeStrs: Array[String] =
            (tableTypeStrsOpt getOrElse tableTypes.keys).toArray

        val result: ResultSet =
            metaData.getTables(catalog, tableSchema, tableName, tableTypeStrs)
        try readMetadata(result) finally { result.close() }
    }
}
