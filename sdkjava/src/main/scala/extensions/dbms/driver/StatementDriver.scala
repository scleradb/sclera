/**
* Sclera Extensions - Java SDK
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

package com.scleradb.java.dbms.driver

import scala.jdk.CollectionConverters._

import com.scleradb.objects.DbObjectDuration

import com.scleradb.sql.expr.{Row, TableRefTarget}
import com.scleradb.sql.objects.Table
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableRow, TableResult}
import com.scleradb.sql.statements.{SqlRelQueryStatement, SqlUpdateStatement}

import com.scleradb.dbms.location.Location
import com.scleradb.dbms.driver.{StatementMetadataDriver => ScalaMetadataDriver}
import com.scleradb.dbms.driver.{StatementDriver => ScalaStatementDriver}

/** Abstract driver for a database management system */
abstract class StatementDriver extends ScalaStatementDriver {
    /** Location object identifying this system */
    override val location: Location
    /** The associated metadata driver */
    override val metadataDriver: ScalaMetadataDriver

    /** Begin transaction */
    override def beginTransaction(): Unit = { /* do nothing */ }
    /** Commit transaction */
    override def commitTransaction(): Unit = { /* do nothing */ }
    /** Abort transaction */
    override def abortTransaction(): Unit = { /* do nothing */ }

    /** Execute queries
      * @param stmt Logical expression to be executed
      * @return TableResult object containing the table result metadata
      *         and row iterator
      */
    override def executeQuery(stmt: SqlRelQueryStatement): TableResult

    /** Execute a native update statement.
      * This statement is not interpreted by Sclera and passed on for execution.
      * @param stmtStr String containing the update statement
      */
    override def executeUpdate(stmtStr: String): Unit

    /** Execute queries
      * @param stmt Update expression to be executed
      * @return The updated table, if available
      */
    override def executeUpdate(stmt: SqlUpdateStatement): Option[Table]

    /** Create a temporary target table, inserts the incoming data
      * and returns the created target table.
      * @param dataColumns List of columns
      * @param dataRows Iterator on rows
      * @param tableName Name of the table to be created
      * @param duration Duration of the created table
      */
    def executeTransferIn(
        dataColumns: Array[Column],
        dataRows: java.util.Iterator[TableRow],
        tableName: String,
        duration: DbObjectDuration
    ): Table

    /** Create a temporary target table, inserts the incoming data
      * and returns the created target table. (Scala)
      * @param dataColumns List of columns
      * @param dataRows Iterator on rows
      * @param tableName Name of the table to be created
      * @param duration Duration of the created table
      */
    override def executeTransferIn(
        dataColumns: List[Column],
        dataRows: Iterator[TableRow],
        tableName: String,
        duration: DbObjectDuration
    ): Table =
        executeTransferIn(
            dataColumns.toArray, dataRows.asJava, tableName, duration
        )

    /** Inserts the incoming data into an existing table at this location.
      * @param rowsIter Iterator on rows to be inserted
      * @param tableRef Target table into which the rows are to be inserted
      */
    def insertData(
        rowsIter: java.util.Iterator[Row],
        tableRef: TableRefTarget
    ): Unit

    /** Inserts the incoming data into an existing table at this location
      * (Scala)
      * @param rowsIter Iterator on rows to be inserted
      * @param tableRef Target table into which the rows are to be inserted
      */
    override def insertData(
        rowsIter: Iterator[Row], tableRef: TableRefTarget
    ): Unit = insertData(rowsIter.asJava, tableRef)

    /** Close the connection */
    override def close(): Unit
}
