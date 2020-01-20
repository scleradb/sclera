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

package com.scleradb.dbms.rdbms.driver

import java.sql.{Array => SqlArray, _}

import com.scleradb.exec.Schema

import com.scleradb.dbms.driver.StatementMetadataDriver

import com.scleradb.objects._

import com.scleradb.sql.objects.{TableId, Table}
import com.scleradb.sql.expr._
import com.scleradb.sql.statements._
import com.scleradb.sql.datatypes.{Column, PrimaryKey, ForeignKey}
import com.scleradb.sql.result.TableResult

private[scleradb]
class SqlMetadataDriver(
    val driver: SqlDriver,
    val metaData: DatabaseMetaData
) extends StatementMetadataDriver {
    val tableTypes: Map[String, (Table.BaseType, DbObjectDuration)] = Map(
        "TABLE" -> (
            Table.BaseTable,
            if( driver.location.isTemporary ) Temporary else Persistent
        ),
        "TEMPORARY TABLE" -> (Table.BaseTable, Temporary),
        "LOCAL TEMPORARY" -> (Table.BaseTable, Temporary),
        "GLOBAL TEMPORARY" -> (Table.BaseTable, Temporary),
        "VIEW" -> (
            Table.BaseView,
            if( driver.location.isTemporary ) Temporary else Persistent
        ),
        "TEMPORARY VIEW" -> (Table.BaseView, Temporary),
        "MATERIALIZED VIEW" -> (
            Table.BaseTable,
            if( driver.location.isTemporary ) Temporary else Persistent
        )
    )

    private def readPrimaryKey(rs: ResultSet): List[(Short, ColRef)] =
        if( rs.next ) {
            val keySeq: Short = rs.getShort("KEY_SEQ")
            val colName: String = rs.getString("COLUMN_NAME")
            (keySeq, ColRef(colName.toUpperCase))::readPrimaryKey(rs)
        } else Nil

    private def primaryKey(tableName: String): Option[PrimaryKey] = {
        val rs: ResultSet =
            metaData.getPrimaryKeys(null, null, tableName)
        val keyColRefsUnordered: List[(Short, ColRef)] =
            try readPrimaryKey(rs) finally rs.close()

        val keyColRefsOrdered: List[(Short, ColRef)] =
            keyColRefsUnordered.sortBy { case (keySeq, _) => keySeq }
        val keyColRefs: List[ColRef] =
            keyColRefsOrdered.map { case (_, colRef) => colRef }

        if( keyColRefs.isEmpty ) None else Some(PrimaryKey(keyColRefs))
    }

    private def readForeignKeys(
        rs: ResultSet
    ): List[(String, Short, (ColRef, ColRef))] = if( rs.next ) {
        val refTableName: String = rs.getString("PKTABLE_NAME")
        val keySeq: Short = rs.getShort("KEY_SEQ")
        val fkColName: String = rs.getString("FKCOLUMN_NAME")
        val refColName: String = rs.getString("PKCOLUMN_NAME")

        val fkRefPair =
            (ColRef(fkColName.toUpperCase), ColRef(refColName.toUpperCase))
        (refTableName.toUpperCase, keySeq, fkRefPair)::readForeignKeys(rs)
    } else Nil

    private def foreignKeys(tableName: String): List[ForeignKey] = {
        val rs: ResultSet = metaData.getImportedKeys(null, null, tableName)
        val fksUnordered: List[(String, Short, (ColRef, ColRef))] =
            try readForeignKeys(rs) finally rs.close()

        val fksOrdered: List[(String, Short, (ColRef, ColRef))] =
            fksUnordered.sortWith { case ((nameA, seqA, _), (nameB, seqB, _)) =>
                (nameA < nameB) || ((nameA == nameB) && seqA < seqB)
            }

        val (_, fksNested) =
            fksOrdered.foldLeft (0, List[(String, List[(ColRef, ColRef)])]()) {
                case ((prevKeySeq, prev@((prevRefTableName, x)::xs)),
                      (refTableName, keySeq, fkRefPair)) =>
                    if( prevKeySeq < keySeq ) { // continued col list
                        assert(prevRefTableName == refTableName)
                        (keySeq, (refTableName, fkRefPair::x)::xs)
                    } else (keySeq, (refTableName, List(fkRefPair))::prev)
                case ((_, Nil), (refTableName, keySeq, fkRefPair)) =>
                    (keySeq, List((refTableName, List(fkRefPair))))
            }

        fksNested.map { case (refTableName, fkRefPairs) =>
            val (fkCols, refCols) = fkRefPairs.unzip
            ForeignKey(fkCols, Some(driver.location.id), refTableName, refCols)
        }
    }

    override def table(tableName: String, tableType: Table.BaseType): Table = {
        val emptyQuery: String =
            "SELECT * FROM " + driver.location.annotTableName(tableName) +
            " WHERE (1=0)"
        val rs: SqlTableResult = driver.executeQuery(emptyQuery)
        val cols: List[Column] = try rs.columns finally { rs.close() }

        val pk: Option[PrimaryKey] = primaryKey(tableName)
        val fks: List[ForeignKey] = foreignKeys(tableName)
        Table(tableName.toUpperCase, cols, pk, fks, tableType)
    }

    override def table(tableName: String): (Table, DbObjectDuration) =
        table(None, driver.location.dbSchemaOpt, tableName, None)

    private def table(
        catalogOpt: Option[String],
        schemaOpt: Option[String],
        tableName: String,
        tableTypeStrsOpt: Option[List[String]]
    ): (Table, DbObjectDuration) =
        tables(catalogOpt, schemaOpt, Some(tableName), tableTypeStrsOpt) match {
            case List(tdur) => tdur

            case Nil =>
                throw new IllegalArgumentException(
                    "Table \"" + tableName + "\" not found"
                )

            case _ =>
                // not expected unless something is wrong underneath
                throw new RuntimeException(
                    "Multiple tables with name \"" + tableName + "\" found"
                )
        }

    override def tables: List[(Table, DbObjectDuration)] =
        tables(None, driver.location.dbSchemaOpt, None, None)

    private def tables(
        catalogOpt: Option[String],
        schemaOpt: Option[String],
        tableNameOpt: Option[String],
        tableTypeStrsOpt: Option[List[String]]
    ): List[(Table, DbObjectDuration)] =
        tablesMetadata(
            catalogOpt, schemaOpt, tableNameOpt, tableTypeStrsOpt
        ).flatMap { case (name, typeStr) =>
            val (tableType, duration) =
                tableTypes.get(typeStr.toUpperCase) getOrElse {
                    throw new RuntimeException(
                        "Unexpected table type: " + typeStr
                    )
                }

            try Some((table(name, tableType), duration))
            catch { case (e: Throwable) => None /* ignore if cannot access */ }
        }

    def tablesMetadata(
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
            catalogOpt.map { name => name.toLowerCase } getOrElse null
        val tableSchema: String =
            tableSchemaOpt.map { name => name.toLowerCase } getOrElse null
        val tableName: String =
            tableNameOpt.map { name => name.toLowerCase } getOrElse null
        val tableTypeStrs: Array[String] =
            (tableTypeStrsOpt getOrElse tableTypes.keys).toArray

        val result: ResultSet =
            metaData.getTables(catalog, tableSchema, tableName, tableTypeStrs)
        try readMetadata(result) finally { result.close() }
    }

    override def close(): Unit = { }
}
