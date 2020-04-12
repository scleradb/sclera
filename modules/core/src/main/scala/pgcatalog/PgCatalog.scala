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

package com.scleradb.pgcatalog

import scala.collection.concurrent.TrieMap

import java.sql.{Types, SQLException}
import org.h2.server.pg.{PgServer => H2PgServer}

import com.scleradb.exec.Schema
import com.scleradb.external.expr.{ExternalSourceExpr, ExternalScalarFunction}

class PgCatalog(schema: Schema) {
    private val pgSourceCache: TrieMap[String, PgCatalogSource] = TrieMap()

    def table(name: String): ExternalSourceExpr = {
        val ucName: String = name.toUpperCase
        val pgSource: PgCatalogSource =
            pgSourceCache.getOrElseUpdate(
                ucName, PgCatalogSource(schema, ucName)
            )

        ExternalSourceExpr(schema, pgSource)
    }

    def function(name: String): ExternalScalarFunction = {
        val ucName: String = name.toUpperCase
        val pgFunction: PgCatalogFunction = PgCatalogFunction(ucName)

        ExternalScalarFunction(pgFunction)
    }
}

object PgCatalog {
    def apply(schema: Schema): PgCatalog = new PgCatalog(schema)

    /** JDBC SQL type codes for the given PostgreSQL type oid
      *
      * @param pgTypeOid PostgreSQL type oid
      * @return The corresponding JDBC SQL type code
      */
    def jdbcType(pgTypeOid: Int): Int = pgTypeOid match {
        case H2PgServer.PG_TYPE_BOOL => Types.BOOLEAN
        case H2PgServer.PG_TYPE_VARCHAR => Types.VARCHAR
        case H2PgServer.PG_TYPE_TEXT => Types.CLOB
        case H2PgServer.PG_TYPE_BPCHAR => Types.CHAR
        case H2PgServer.PG_TYPE_INT2 => Types.SMALLINT
        case H2PgServer.PG_TYPE_INT4 => Types.INTEGER
        case H2PgServer.PG_TYPE_INT8 => Types.BIGINT
        case H2PgServer.PG_TYPE_NUMERIC => Types.DECIMAL
        case H2PgServer.PG_TYPE_FLOAT4 => Types.REAL
        case H2PgServer.PG_TYPE_FLOAT8 => Types.DOUBLE
        case H2PgServer.PG_TYPE_TIME => Types.TIME
        case H2PgServer.PG_TYPE_DATE => Types.DATE
        case H2PgServer.PG_TYPE_TIMESTAMP_NO_TMZONE => Types.TIMESTAMP
        case H2PgServer.PG_TYPE_BYTEA => Types.VARBINARY
        case H2PgServer.PG_TYPE_OID => Types.BLOB
        case H2PgServer.PG_TYPE_TEXTARRAY => Types.ARRAY
        case _ =>
            throw new SQLException(
                "Type (PG code " + pgTypeOid + ") not supported", "08P01"
            )
    }
}
