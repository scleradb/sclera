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

import java.util.Properties
import java.sql.{DriverManager, Connection, Statement, ResultSet, SQLException}

import org.h2.server.pg.{PgServer => H2PgServer}

import scala.collection.concurrent.TrieMap

import com.scleradb.exec.Schema

import com.scleradb.dbms.location.LocationId
import com.scleradb.dbms.rdbms.driver.SqlTableResult

import com.scleradb.sql.expr._
import com.scleradb.sql.types._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.objects.RelationId
import com.scleradb.sql.result.{TableResult, TableRow, ScalTableRow}

import com.scleradb.external.objects.ExternalSource

/** PG Catalog data source - base class */
sealed abstract class PgCatalogSource extends ExternalSource {
    /** Name of the data source */
    override val name: String = PgCatalogSource.name

    /** Name of the virtual catalog table */
    val tableName: String

    /** Columns of the result (virtual table) */
    override val columns: List[Column]

    /** Rows of the result (virtual table) */
    def rows: List[ScalTableRow]

    /** Formatted result */
    override def result: TableResult = new PgCatalogResult(columns, rows)

    /** String used for this source in the EXPLAIN output */
    override def toString: String = name + "." + tableName
}

/** Table PG_TYPE */
object PgType extends PgCatalogSource {
    override val tableName: String = "PG_TYPE"

    override val columns: List[Column] = List(
        Column("OID", SqlInteger),
        Column("TYPNAME", SqlCharVarying(None)),
        Column("TYPNAMESPACE", SqlInteger),
        Column("TYPLEN", SqlSmallInt),
        Column("TYPTYPE", SqlCharFixed(Some(1))),
        Column("TYPBASETYPE", SqlInteger),
        Column("TYPTYPMOD", SqlInteger),
        Column("TYPNOTNULL", SqlBool),
        Column("TYPINPUT", SqlCharVarying(None).option)
    )

    private lazy val baseRows: List[ScalTableRow] = {
        val baseQuery: String = """
            select
                data_type,
                cast(type_name as varchar_ignorecase) typname,
                %d typnamespace,
                -1 typlen,
                'c' typtype,
                0 typbasetype,
                -1 typtypmod,
                false typnotnull,
                null typinput
            from INFORMATION_SCHEMA.type_info
            where pos = 0;
        """.format(PgCatalogSource.scleraOid)

        val jdbcUrl: String =
            "jdbc:h2:mem:sclera_pg_catalog;IGNORECASE=TRUE;MODE=PostgreSQL"

        val conn: Connection =
            DriverManager.getConnection(jdbcUrl, new Properties())
        try {
            val stmt: Statement = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
            )

            // the SqlTableResult object is responsible for closing the statement
            val ts: SqlTableResult = SqlTableResult(stmt, baseQuery, Nil)
            val rs: List[TableRow] = try ts.rows.toList finally ts.close()

            rs.flatMap { case row =>
                val outCols: List[Column] =
                    columns.filter { col => (col.name != "OID") }
                val colValMap: Map[String, ScalColValue] =
                    row.getScalExprMap(outCols)

                val oid: Int = row.getIntOpt("DATA_TYPE") match {
                    case Some(sqlType) => H2PgServer.convertType(sqlType)
                    case None =>
                        throw new SQLException("Data type not found", "22000")
                }

                if( oid == 705 ) None else Some(
                    ScalTableRow(colValMap + ("OID" -> IntConst(oid)))
                )
            }
        } finally conn.close()
    }

    private val addition: List[ScalTableRow] = List(
        List(
            IntConst(19),
            CharConst("name"),
            IntConst(0),
            ShortConst(-1),
            CharConst("c"),
            IntConst(0),
            IntConst(-1),
            BoolConst(false),
            SqlNull(SqlCharVarying(None))
        ),
        List(
            IntConst(0),
            CharConst("null"),
            IntConst(0),
            ShortConst(-1),
            CharConst("c"),
            IntConst(0),
            IntConst(-1),
            BoolConst(false),
            SqlNull(SqlCharVarying(None))
        ),
        List(
            IntConst(22),
            CharConst("int2vector"),
            IntConst(0),
            ShortConst(-1),
            CharConst("c"),
            IntConst(0),
            IntConst(-1),
            BoolConst(false),
            SqlNull(SqlCharVarying(None))
        ),
        List(
            IntConst(2205),
            CharConst("regproc"),
            IntConst(0),
            ShortConst(4),
            CharConst("b"),
            IntConst(0),
            IntConst(-1),
            BoolConst(false),
            SqlNull(SqlCharVarying(None))
        )
    ).map { vs =>
        val colVals: List[(String, ScalColValue)] =
            columns.zip(vs).map { case (col, v) => col.name -> v }
        ScalTableRow(colVals)
    }

    override lazy val rows: List[ScalTableRow] = addition ::: baseRows
}

/** Table PG_NAMESPACE */
class PgNameSpace(schema: Schema) extends PgCatalogSource {
    override val tableName: String = "PG_NAMESPACE"

    /** Columns of the result (virtual table) */
    override val columns: List[Column] = List(
        Column("OID", SqlInteger),
        Column("NSPNAME", SqlCharVarying(None)),
        Column("NSPOWNER", SqlInteger)
    )

    private val scleraRow: ScalTableRow =
        ScalTableRow(
            "OID" -> IntConst(PgCatalogSource.scleraOid),
            "NSPNAME" -> CharConst("sclera"),
            "NSPOWNER" -> IntConst(0)
        )

    private lazy val locationRows: List[ScalTableRow] =
        schema.locationIds.map { locId =>
            ScalTableRow(
                "OID" -> IntConst(PgCatalogSource.locOid(locId)),
                "NSPNAME" -> CharConst(locId.repr.toLowerCase),
                "NSPOWNER" -> IntConst(0)
            )
        }

    /** Rows of the result (virtual table) */
    override lazy val rows: List[ScalTableRow] = scleraRow :: locationRows
}

/** Table PG_CLASS */
class PgClass(schema: Schema) extends PgCatalogSource {
    override val tableName: String = "PG_CLASS"

    /** Columns of the result (virtual table) */
    override val columns: List[Column] = List(
        Column("OID", SqlInteger),
        Column("RELNAME", SqlCharVarying(None)),
        Column("RELNAMESPACE", SqlInteger),
        Column("RELTYPE", SqlInteger),
        Column("RELOFTYPE", SqlInteger),
        Column("RELOWNER", SqlInteger),
        Column("RELAM", SqlInteger),
        Column("RELFILENODE", SqlInteger),
        Column("RELTABLESPACE", SqlInteger),
        Column("RELPAGES", SqlInteger),
        Column("RELTUPLES", SqlReal),
        Column("RELALLVISIBLE", SqlInteger),
        Column("RELTOASTRELID", SqlInteger),
        Column("RELTOASTIDXID", SqlInteger),
        Column("RELHASINDEX", SqlBool),
        Column("RELISSHARED", SqlBool),
        Column("RELPERSISTENCE", SqlCharFixed(Some(1))),
        Column("RELKIND", SqlCharFixed(Some(1))),
        Column("RELNATTS", SqlSmallInt),
        Column("RELCHECKS", SqlSmallInt),
        Column("RELHASOIDS", SqlBool),
        Column("RELHASPKEY", SqlBool),
        Column("RELHASRULES", SqlBool),
        Column("RELHASTRIGGERS", SqlBool),
        Column("RELHASSUBCLASS", SqlBool),
        Column("RELISPOPULATED", SqlBool),
        Column("RELFROZENXID", SqlInteger),
        Column("RELMINMXID", SqlInteger)
    )

    private def tables: List[ScalTableRow] =
        schema.tables.filter(
            st => !st.name.startsWith("SCLERATEMP_")
        ).map { st =>
            ScalTableRow(
                "OID" -> IntConst(PgCatalogSource.relOid(st.id)),
                "RELNAME" -> CharConst(st.obj.name.toLowerCase),
                "RELNAMESPACE" ->
                    IntConst(PgCatalogSource.locOid(st.id.locationId)),
                "RELTYPE" -> IntConst(0),
                "RELOFTYPE" -> IntConst(0),
                "RELOWNER" -> IntConst(0),
                "RELAM" -> IntConst(0),
                "RELFILENODE" -> IntConst(0),
                "RELTABLESPACE" -> IntConst(0),
                "RELPAGES" -> IntConst(1),
                "RELTUPLES" -> FloatConst(1.0f),
                "RELALLVISIBLE" -> IntConst(1),
                "RELTOASTRELID" -> IntConst(0),
                "RELTOASTIDXID" -> IntConst(0),
                "RELHASINDEX" -> BoolConst(false),
                "RELISSHARED" -> BoolConst(false),
                "RELPERSISTENCE" -> CharConst(
                    if( schema.isTemporary(st.id.repr) ) "t" else "p"
                ),
                "RELKIND" -> CharConst("r"),
                "RELNATTS" -> IntConst(st.obj.columnRefs.size),
                "RELCHECKS" -> IntConst(0),
                "RELHASOIDS" -> BoolConst(false),
                "RELHASPKEY" -> BoolConst(!st.obj.keyOpt.isEmpty),
                "RELHASRULES" -> BoolConst(false),
                "RELHASTRIGGERS" -> BoolConst(false),
                "RELHASSUBCLASS" -> BoolConst(false),
                "RELISPOPULATED" -> BoolConst(true),
                "RELFROZENXID" -> IntConst(0),
                "RELMINMXID" -> IntConst(0)
            )
        }

    private def views: List[ScalTableRow] =
        schema.views.map { sv =>
            ScalTableRow(
                "OID" -> IntConst(PgCatalogSource.relOid(sv.id)),
                "RELNAME" -> CharConst(sv.obj.name.toLowerCase),
                "RELNAMESPACE" -> IntConst(PgCatalogSource.scleraOid),
                "RELTYPE" -> IntConst(0),
                "RELOFTYPE" -> IntConst(0),
                "RELOWNER" -> IntConst(0),
                "RELAM" -> IntConst(0),
                "RELFILENODE" -> IntConst(0),
                "RELTABLESPACE" -> IntConst(0),
                "RELPAGES" -> IntConst(1),
                "RELTUPLES" -> FloatConst(1.0f),
                "RELALLVISIBLE" -> IntConst(1),
                "RELTOASTRELID" -> IntConst(0),
                "RELTOASTIDXID" -> IntConst(0),
                "RELHASINDEX" -> BoolConst(false),
                "RELISSHARED" -> BoolConst(false),
                "RELPERSISTENCE" -> CharConst(
                    if( schema.isTemporary(sv.id.repr) ) "t" else "p"
                ),
                "RELKIND" -> CharConst("v"),
                "RELNATTS" -> IntConst(sv.obj.columnRefs.size),
                "RELCHECKS" -> IntConst(0),
                "RELHASOIDS" -> BoolConst(false),
                "RELHASPKEY" -> BoolConst(false),
                "RELHASRULES" -> BoolConst(false),
                "RELHASTRIGGERS" -> BoolConst(false),
                "RELHASSUBCLASS" -> BoolConst(false),
                "RELISPOPULATED" -> BoolConst(true),
                "RELFROZENXID" -> IntConst(0),
                "RELMINMXID" -> IntConst(0)
            )
        }

    override def rows: List[ScalTableRow] = tables:::views
}

/** Table PG_ATTRIBUTE */
class PgAttribute(schema: Schema) extends PgCatalogSource {
    override val tableName: String = "PG_CLASS"

    /** Columns of the result (virtual table) */
    override val columns: List[Column] = List(
        Column("OID", SqlInteger),
        Column("ATTRELID", SqlInteger),
        Column("ATTNAME", SqlCharVarying(None)),
        Column("ATTTYPID", SqlInteger),
        Column("ATTLEN", SqlSmallInt),
        Column("ATTNUM", SqlSmallInt),
        Column("ATTTYPMOD", SqlInteger),
        Column("ATTNOTNULL", SqlBool),
        Column("ATTISDROPPED", SqlBool),
        Column("ATTHASDEF", SqlBool)
    )

    private def tableRows: List[ScalTableRow] =
        schema.tables.filter(
            st => !st.name.startsWith("SCLERATEMP_")
        ).flatMap { st =>
            val tOid: Int = PgCatalogSource.relOid(st.id)

            st.obj.columns.zipWithIndex.map { case (col, n) =>
                val index: Short = (n + 1).toShort
                val sqlType: SqlType = col.sqlType
                val pgType: Int = H2PgServer.convertType(sqlType.sqlTypeCode)
                val pgTypeLen: Short = pgTypeLength.get(pgType).getOrElse(-1)

                ScalTableRow(
                    "OID" -> IntConst(tOid*100000 + index),
                    "ATTRELID" -> IntConst(tOid),
                    "ATTNAME" -> CharConst(col.name.toLowerCase),
                    "ATTTYPID" -> IntConst(pgType),
                    "ATTLEN" -> ShortConst(pgTypeLen),
                    "ATTNUM" -> ShortConst(index),
                    "ATTTYPMOD" -> IntConst(-1),
                    "ATTNOTNULL" -> BoolConst(!sqlType.isOption),
                    "ATTISDROPPED" -> BoolConst(false),
                    "ATTHASDEF" -> BoolConst(false)
                )
            }
        }

    private def viewRows: List[ScalTableRow] =
        schema.views.flatMap { sv =>
            val vOid: Int = PgCatalogSource.relOid(sv.id)

            val viewColumns: List[Column] = schema.columns(sv.obj.expr)
            viewColumns.zipWithIndex.map { case (col, n) =>
                val index: Short = (n + 1).toShort
                val sqlType: SqlType = col.sqlType
                val pgType: Int = H2PgServer.convertType(sqlType.sqlTypeCode)
                val pgTypeLen: Short = pgTypeLength.get(pgType).getOrElse(-1)

                ScalTableRow(
                    "OID" -> IntConst(vOid*100000 + index),
                    "ATTRELID" -> IntConst(vOid),
                    "ATTNAME" -> CharConst(col.name.toLowerCase),
                    "ATTTYPID" -> IntConst(pgType),
                    "ATTLEN" -> ShortConst(pgTypeLen),
                    "ATTNUM" -> ShortConst(index),
                    "ATTTYPMOD" -> IntConst(-1),
                    "ATTNOTNULL" -> BoolConst(!sqlType.isOption),
                    "ATTISDROPPED" -> BoolConst(false),
                    "ATTHASDEF" -> BoolConst(false)
                )
            }
        }

    override def rows: List[ScalTableRow] = tableRows:::viewRows

    private lazy val pgTypeLength: Map[Int, Short] = Map() ++
        PgType.rows.flatMap { r =>
            (r.getIntOpt("OID"), r.getShortOpt("TYPLEN")) match {
                case (Some(pgType), Some(len)) => Some(pgType -> len)
                case _ => None
            }
        }
}

/** Table PG_ATTRDEF */
object PgAttrDef extends PgCatalogSource {
    override val tableName: String = "PG_ATTRDEF"

    /** Columns of the result (virtual table) */
    override val columns: List[Column] = List(
        Column("OID", SqlInteger),
        Column("ADRELID", SqlInteger),
        Column("ADNUM", SqlSmallInt),
        Column("ADBIN", SqlCharVarying(None)),
        Column("ADSRC", SqlCharVarying(None))
    )

    /** Rows of the result (virtual table) */
    override val rows: List[ScalTableRow] = Nil
}

object PgCatalogSource {
    /** The data source name */
    val name: String = "PG_CATALOG"

    /** OID Map */
    private val oidMap: TrieMap[String, Int] = TrieMap("SCLERA" -> 0)
    private def oid(id: String): Int = oidMap.getOrElseUpdate(id, oidMap.size)

    def relOid(id: RelationId): Int = oid(id.repr)

    def locOid(id: LocationId): Int = oid(id.repr)

    val scleraOid: Int = oidMap("SCLERA")

    def apply(
        schema: Schema,
        tableName: String
    ): PgCatalogSource = tableName.toUpperCase match {
        case "PG_TYPE" => PgType
        case "PG_NAMESPACE" => new PgNameSpace(schema)
        case "PG_CLASS" => new PgClass(schema)
        case "PG_ATTRIBUTE" => new PgAttribute(schema)
        case "PG_ATTRDEF" => PgAttrDef
        case other =>
            throw new IllegalArgumentException(
                "Table not found: " + name + "." + tableName
            )
    }
}
