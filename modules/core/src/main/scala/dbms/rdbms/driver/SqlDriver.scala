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

import java.util.Properties
import java.sql.{DriverManager, Connection}
import java.sql.{Statement, ResultSet, SQLException}

import scala.collection.concurrent.TrieMap

import com.zaxxer.hikari.{HikariConfig, HikariDataSource}

import org.slf4j.{Logger, LoggerFactory}

import com.scleradb.objects.DbObjectDuration

import com.scleradb.dbms.location._
import com.scleradb.dbms.driver._

import com.scleradb.sql.statements._
import com.scleradb.sql.objects.{TableId, Table, SchemaTable}
import com.scleradb.sql.expr._
import com.scleradb.sql.mapper.SqlMapper
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableRow

import com.scleradb.config.ScleraConfig

private[scleradb]
abstract class SqlDriver extends StatementDriver {
    val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

    val sqlMapper: SqlMapper
    val jdbcUrl: String
    val configProps: List[(String, String)]

    protected val conn: Connection = createConnection()

    override val metadataDriver: StatementMetadataDriver =
        new SqlMetadataDriver(this, conn.getMetaData())

    private def createConnection(): Connection = {
        val msg: String = "Creating connection: " + jdbcUrl

        if( ScleraConfig.isExplain ) printConsole(msg)
        logger.info(msg)

        try SqlDriver.connection(jdbcUrl, configProps)
        catch { case e: SQLException =>
            logger.error("SQL Exception: " + e.getMessage())
            throw e
        }
    }

    def createQueryStatement(): Statement =
        try conn.createStatement(
            ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
        ) catch { case e: SQLException =>
            logger.error("SQL Exception: " + e.getMessage())
            logger.error(e.getStackTrace.mkString("\n"))
            throw e
        }

    private def createUpdateStatement(): Statement =
        try conn.createStatement() catch { case e: SQLException =>
            logger.error("SQL Exception: " + e.getMessage())
            logger.error(e.getStackTrace.mkString("\n"))
            throw e
        }

    override def beginTransaction(): Unit =
        conn.setAutoCommit(false)

    override def commitTransaction(): Unit = {
        conn.commit()
        conn.setAutoCommit(true)
    }

    override def abortTransaction(): Unit = {
        conn.rollback()
        conn.setAutoCommit(true)
    }

    def executeQuery(
        stmtStr: String,
        resultOrder: List[SortExpr] = Nil
    ): SqlTableResult = {
        if( ScleraConfig.isExplain ) printConsole(stmtStr)

        val queryStatement: Statement = createQueryStatement()

        // the SqlTableResult object is responsible for closing the statement
        try SqlTableResult(queryStatement, stmtStr, resultOrder)
        catch { case e: SQLException =>
            logger.error("SQL Exception [" + stmtStr + "]: " + e.getMessage())
            logger.error(e.getStackTrace.take(20).mkString("\n"))
            throw e
        }
    }

    override def executeQuery(stmt: SqlRelQueryStatement): SqlTableResult =
        executeQuery(sqlMapper.queryString(stmt), stmt.resultOrder)

    def executeUpdate(stmtStr: String, updateStatement: Statement): Unit = {
        if( ScleraConfig.isExplain ) printConsole(stmtStr)
        if( !location.isWritable ) throw new IllegalArgumentException(
            "Location \"" + location.id.repr + "\" is read-only"
        )

        try updateStatement.executeUpdate(stmtStr)
        catch { case e: SQLException =>
            logger.error("SQL Exception [" + stmtStr + "]: " + e.getMessage())
            logger.error(e.getStackTrace.mkString("\n"))
            throw e
        }
    }

    override def executeUpdate(stmtStr: String): Unit = {
        val updateStatement: Statement = createUpdateStatement()
        try executeUpdate(stmtStr, updateStatement)
        finally updateStatement.close()
    }

    private def executeUpdate(
        stmt: SqlUpdateStatement,
        updateStatement: Statement
    ): Unit = sqlMapper.updateString(stmt).foreach { stmtStr =>
        executeUpdate(stmtStr, updateStatement)
    }

    override def executeUpdate(stmt: SqlUpdateStatement): Option[Table] = {
        val updateStatement: Statement = createUpdateStatement()
        try executeUpdate(stmt, updateStatement) finally updateStatement.close()

        None
    }

    // creates a temporary target table and inserts the incoming data
    // returns the target table
    override def executeTransferIn(
        dataColumns: List[Column],
        dataRows: Iterator[TableRow],
        tableName: String,
        duration: DbObjectDuration
    ): Table = {
        val table: Table =
            Table(tableName, dataColumns, None, Nil, Table.BaseTable)
        val schemaTable: SchemaTable = SchemaTable(table, location.id)
        val tableRef: TableRefTarget =
            TableRefTargetExplicit(location.schema, schemaTable)

        val updateStatement: Statement = createUpdateStatement()
        try {
            // create the target table
            val createStmt: SqlCreateDbObject =
                SqlCreateDbObject(SqlTable(table), duration)
            executeUpdate(createStmt, updateStatement)

            try {
                // insert the data
                val rowsIter: Iterator[Row] = dataRows.map { t =>
                    val values: List[ScalColValue] =
                        dataColumns.map { col => t.getScalExpr(col) }

                    Row(values)
                }

                insertData(rowsIter, tableRef, updateStatement)
            } catch { case (e: Throwable) =>
                // insert failed -- drop the table created earlier
                val dropStmt: SqlDrop = SqlDropExplicit(schemaTable, duration)
                executeUpdate(dropStmt, updateStatement)

                throw e
            }
        } finally updateStatement.close()

        table
    }

    private def insertData(
        rowsIter: Iterator[Row],
        tableRef: TableRefTarget,
        updateStatement: Statement
    ): Unit = {
        val valuesAlias: TableAlias = TableAlias("V")
        rowsIter.grouped(ScleraConfig.batchSize).foreach { rowBatch =>
            val stmt: SqlInsert = SqlInsert(tableRef, rowBatch.toList)
            executeUpdate(stmt, updateStatement)
        }
    }

    override def insertData(
        rowsIter: Iterator[Row],
        tableRef: TableRefTarget
    ): Unit = {
        val updateStatement: Statement = createUpdateStatement()
        try insertData(rowsIter, tableRef, updateStatement)
        finally updateStatement.close()
    }

    override def createDbSchema(dbSchema: String): Unit =
        executeUpdate(SqlCreateDbSchema(dbSchema))

    override def close(): Unit = {
        logger.info("Closing connection: " + jdbcUrl)
        conn.close()
    }
}

object SqlDriver {
    val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)
    private val dsMap: TrieMap[String, HikariDataSource] = TrieMap()

    def connection(
        jdbcUrl: String,
        props: List[(String, String)]
    ): Connection = {
        val dsId: String = jdbcUrl + "::" + props.sorted.mkString
        val ds: HikariDataSource = dsMap.getOrElseUpdate(dsId, {
            val config: HikariConfig = new HikariConfig()
            config.setJdbcUrl(jdbcUrl)
            config.setMinimumIdle(0)
            config.setMaximumPoolSize(20)
            props.foreach { case (k, v) => config.addDataSourceProperty(k, v) }

            logger.info("Initializing pool: " + dsId)
            new HikariDataSource(config)
        })

        ds.getConnection()
    }

    def closeConnectionPools(): Unit = {
        dsMap.foreach { case (dsId, ds) =>
            logger.info("Closing pool: " + dsId)
            ds.close()
        }

        dsMap.clear()
    }
}
