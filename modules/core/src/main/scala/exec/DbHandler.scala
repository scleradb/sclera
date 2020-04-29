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

package com.scleradb.exec

import com.scleradb.objects._

import com.scleradb.dbms.location._
import com.scleradb.dbms.driver._

import com.scleradb.sql.objects.Table
import com.scleradb.sql.expr._
import com.scleradb.sql.statements._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, TableRow}

/** Handler for the given location */
class DbHandler(val location: Location) {
    private val driver: StatementDriver =
        try location.driver catch { case (e: Throwable) =>
            throw new IllegalArgumentException(
                "Could not initialize driver for " + location.id.repr +
                " (" + location.dbms + "): " + e.getMessage(),
                e
            )
        }

    private val metadataDriver: StatementMetadataDriver = driver.metadataDriver

    def table(name: String, tableType: Table.BaseType): Table =
        metadataDriver.table(name, tableType)

    def table(name: String): (Table, DbObjectDuration) =
        metadataDriver.table(name)

    def tables: List[(Table, DbObjectDuration)] = metadataDriver.tables

    def beginTransaction(): Unit = driver.beginTransaction()
    def commitTransaction(): Unit = driver.commitTransaction()
    def abortTransaction(): Unit = driver.abortTransaction()

    def inTransaction[T](f: => T): T = {
        beginTransaction()

        val result: T = try f catch {
            case (e: Throwable) =>
                abortTransaction()
                throw e
        }

        commitTransaction()

        result
    }

    def handleUpdate(stmt: SqlUpdateStatement): Option[Table] =
        driver.executeUpdate(stmt)

    def executeNativeStatement(stmtStr: String): Unit =
        driver.executeUpdate(stmtStr)

    // create a table and transfer data
    def handleTransferIn(
        dataColumns: List[Column],
        dataRows: Iterator[TableRow],
        tableName: String,
        duration: DbObjectDuration
    ): Table = inTransaction {
        driver.executeTransferIn(
            dataColumns, dataRows, tableName, duration
        )
    }

    // transfer data into existing table
    def handleInsertIn(
        dataColumns: List[Column],
        dataRows: Iterator[TableRow],
        tableRef: TableRefTarget
    ): Unit = driver.executeInsertIn(dataColumns, dataRows, tableRef)

    def queryResult(relExpr: RelExpr): TableResult =
        driver.executeQuery(SqlRelQueryStatement(relExpr))

    def createByteStore(): Unit = driver.createByteStore()

    def dropByteStore(): Unit = driver.dropByteStore()

    def storeBytes(id: String, bytes: Array[Byte]): Unit =
        driver.storeBytes(id, bytes)

    def readBytes(id: String): Array[Byte] =
        driver.readBytes(id)

    def removeBytes(id: String): Unit =
        driver.removeBytes(id)

    def createDbSchema(): Unit =
        location.dbSchemaOpt.foreach { dbSchema =>
            driver.createDbSchema(dbSchema)
        }

    def close(): Unit = driver.close()
}

/** Companion object for class `DbHandler` */
object DbHandler {
    def apply(location: Location): DbHandler = new DbHandler(location)

    def get(location: Location): Option[DbHandler] =
        try Some(apply(location)) catch { case (e: Throwable) => None }

    def dbHandlers(schema: Schema): List[DbHandler] = schema.dbHandlers
    def dbHandlerOpt(schema: Schema, id: String): Option[DbHandler] =
        schema.dbHandlerOpt(id)
    def dbHandlerOpt(schema: Schema, id: LocationId): Option[DbHandler] =
        schema.dbHandlerOpt(id)
}
