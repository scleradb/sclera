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

package com.scleradb.dbms.driver

import com.scleradb.objects.DbObjectDuration

import com.scleradb.sql.expr.{TableRefTarget, ColRef, ScalColValue, Row}
import com.scleradb.sql.objects.Table
import com.scleradb.sql.statements.{SqlRelQueryStatement, SqlUpdateStatement}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableRow, TableResult}

import com.scleradb.dbms.location.Location

/** Abstract driver for a database management system */
abstract class StatementDriver {
    /** Location object identifying this system */
    val location: Location

    /** The associated metadata driver */
    val metadataDriver: StatementMetadataDriver

    /** Begin transaction */
    def beginTransaction(): Unit = { /* do nothing */ }
    /** Commit transaction */
    def commitTransaction(): Unit = { /* do nothing */ }
    /** Abort transaction */
    def abortTransaction(): Unit = { /* do nothing */ }

    /** Execute queries
      * @param stmt Logical expression to be executed
      * @return TableResult object containing the table result metadata
      *         and row iterator
      */
    def executeQuery(stmt: SqlRelQueryStatement): TableResult

    /** Execute a native update statement.
      * This statement is not interpreted by Sclera and passed on for execution.
      * @param stmtStr String containing the update statement
      */
    def executeUpdate(stmtStr: String): Unit

    /** Execute queries
      * @param stmt Update expression to be executed
      * @return The updated table, if available
      */
    def executeUpdate(
        stmt: SqlUpdateStatement
    ): Option[Table]

    /** Create a temporary target table, inserts the incoming data
      * and returns the created target table.
      * @param dataColumns List of columns
      * @param dataRows Iterator on rows
      * @param tableName Name of the table to be created
      * @param duration Duration of the created table
      */
    def executeTransferIn(
        dataColumns: List[Column],
        dataRows: Iterator[TableRow],
        tableName: String,
        duration: DbObjectDuration
    ): Table

    /** Inserts the incoming data into an existing table at this location.
      * @param dataColumns List of columns
      * @param dataRows Iterator on rows to be inserted
      * @param tableRef Target table into which the rows are to be inserted
      */
    private[scleradb]
    def executeInsertIn(
        dataColumns: List[Column],
        dataRows: Iterator[TableRow],
        tableRef: TableRefTarget
    ): Unit = {
        // insert the data
        val rowsIter: Iterator[Row] =
            dataRows.map { t =>
                val values: List[ScalColValue] =
                    dataColumns.map { col => t.getScalExpr(col) }

                Row(values)
            }

        insertData(rowsIter, tableRef)
    }

    /** Inserts the incoming data into an existing table at this location.
      * @param rowsIter Iterator on rows to be inserted
      * @param tableRef Target table into which the rows are to be inserted
      */
    def insertData(
        rowsIter: Iterator[Row],
        tableRef: TableRefTarget
    ): Unit

    /** Print a message on the console */
    private[scleradb]
    def printConsole(s: String): Unit =
        println("[%s] %s".format(location.id.repr, s))

    /** Create byte store */
    def createByteStore(): Unit

    /** Drop byte store */
    def dropByteStore(): Unit

    /** Store bytes in the byte store, associated with the given identifier
      * @param id Identifier to associate with the stored byte array
      * @param bytes The byte array to be stored
      */
    def storeBytes(id: String, bytes: Array[Byte]): Unit

    /** Read bytes from the byte store associated with the given identifier
      * @param id Identifier associated with the stored byte array
      * @return The retrieved byte array
      */
    def readBytes(id: String): Array[Byte]

    /** Delete bytes from the byte store associated with the given identifier
      * @param id Identifier associated with the stored byte array
      */
    def removeBytes(id: String): Unit

    /** Create database schema
      * Needed if the location is used for storing schema or as the cache.
      * The schema is used for isolating the users.
      * @param dbSchema Schema to create within the underlying database
      */
    def createDbSchema(dbSchema: String): Unit

    /** Close the connection */
    def close(): Unit
}
