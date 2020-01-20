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

import com.scleradb.exec.Schema

import com.scleradb.sql.expr._
import com.scleradb.sql.types._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.objects.RelationId
import com.scleradb.sql.result.{TableResult, ScalTableRow}

import com.scleradb.external.objects.ExternalFunction

/** PG Catalog functions - base class */
private[scleradb]
sealed abstract class PgCatalogFunction extends ExternalFunction {
    /** String used for this source in the EXPLAIN output */
    override def toString: String = "PG_CATALOG." + name
}

/** Function PG_GET_USERBYID */
private[scleradb]
object PgGetUserById extends PgCatalogFunction {
    override val name: String = "PG_GET_USERBYID"

    override val resultType: SqlType = SqlCharVarying(None)

    override def result(args: List[ScalColValue]): ScalColValue = args match {
        case List(v: IntegralConst) if v.integralValue == 0 =>
            CharConst("sclera")
        case other =>
            throw new IllegalArgumentException(
                "Invalid OID: " + other.map(s => s.repr).mkString(", ")
            )
    }
}

/** Function PG_TABLE_IS_VISIBLE */
private[scleradb]
object PgTableIsVisible extends PgCatalogFunction {
    override val name: String = "PG_TABLE_IS_VISIBLE"

    override val resultType: SqlType = SqlBool

    override def result(args: List[ScalColValue]): ScalColValue = args match {
        case List(_) => BoolConst(true)
        case other =>
            throw new IllegalArgumentException(
                "Invalid OID: " + other.map(s => s.repr).mkString(", ")
            )
    }
}

/** Function FORMAT_TYPE */
private[scleradb]
object PgFormatType extends PgCatalogFunction {
    override val name: String = "FORMAT_TYPE"

    override val resultType: SqlType = SqlCharVarying(None)

    override def result(args: List[ScalColValue]): ScalColValue = args match {
        case List(v: IntegralConst) =>
            val jdbcType: Int = PgCatalog.jdbcType(v.integralValue.toInt)
            CharConst(SqlType(jdbcType).repr.toLowerCase)
        case other =>
            throw new IllegalArgumentException(
                "Invalid Type OID: " + other.map(s => s.repr).mkString(", ")
            )
    }
}

private[scleradb]
class PgNullFunction(
    override val name: String,
    override val resultType: SqlType
) extends PgCatalogFunction {
    override def result(args: List[ScalColValue]): ScalColValue =
        SqlNull(resultType)
}

private[scleradb]
object PgCatalogFunction {
    /** The function source name */
    val name: String = "PG_CATALOG"

    def apply(
        functionName: String
    ): PgCatalogFunction = functionName.toUpperCase match {
        case "PG_GET_USERBYID" => PgGetUserById
        case "PG_TABLE_IS_VISIBLE" => PgTableIsVisible
        case "PG_GET_EXPR" =>
            new PgNullFunction("PG_GET_EXPR", SqlCharVarying(None))
        case "FORMAT_TYPE" => PgFormatType
        case other =>
            throw new IllegalArgumentException(
                "Function not found: " + name + "." + functionName
            )
    }
}
