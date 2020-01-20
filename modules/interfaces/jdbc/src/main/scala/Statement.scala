/**
* Sclera - JDBC Driver
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

package com.scleradb.interfaces.jdbc

import scala.language.postfixOps

import java.sql.{SQLException, SQLWarning}
import java.sql.{SQLFeatureNotSupportedException, SQLDataException}
import java.sql.BatchUpdateException

import java.sql.ResultSet.{TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, FETCH_FORWARD}
import java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT
import java.sql.Statement.{CLOSE_CURRENT_RESULT, SUCCESS_NO_INFO}

import scala.collection.mutable

import com.scleradb.sql.plan.RelEvalPlan
import com.scleradb.sql.types.SqlType
import com.scleradb.sql.statements._

class Statement(
    isReadOnlyStatus: Boolean,
    connection: Connection
) extends java.sql.Statement {
    private var isClosedStatus: Boolean = false
    private val warnings: mutable.Queue[SQLWarning] = mutable.Queue()
    private val batch: mutable.Queue[SqlStatement] = mutable.Queue()

    private var maxRows: Int = 0 // no limit
    private def planQuery(qs: SqlRelQueryStatement): RelEvalPlan = {
        val maxRowsOpt: Option[Int] = if( maxRows <= 0 ) None else Some(maxRows)
        connection.processor.planQuery(qs, maxRowsOpt)
    }

    private object Result {
        sealed abstract class StatementResult
        case class QueryResult(rs: java.sql.ResultSet) extends StatementResult
        case class UpdateResult(countOpt: Option[Int]) extends StatementResult

        private var resultOpt: Option[StatementResult] = None

        def set(rs: java.sql.ResultSet): Unit = {
            resultOpt = Some(QueryResult(rs))
        }

        def set(cntOpt: Option[Int]): Unit = {
            resultOpt = Some(UpdateResult(cntOpt))
        }

        def clear(): Unit = {
            resultOpt.foreach {
                case QueryResult(rs) => if( !rs.isClosed() ) rs.close()
                case _ => ()
            }

            resultOpt = None
        }

        def isActive: Boolean = resultOpt match {
            case Some(QueryResult(rs)) => !rs.isClosed()
            case _ => false
        }

        def isQueryResult: Boolean = resultOpt match {
            case Some(QueryResult(_)) => true
            case _ => false
        }

        def resultSetOpt: Option[java.sql.ResultSet] = resultOpt match {
            case Some(QueryResult(resultSet)) => Some(resultSet)
            case _ => None
        }

        def updateCountOpt: Option[Int] = resultOpt match {
            case Some(UpdateResult(countOpt)) => countOpt orElse Some(0)
            case _ => None
        }
    }

    override def addBatch(sql: String): Unit =
        try { batch ++= connection.processor.parser.parseSqlStatements(sql) }
        catch {
            case (e: SQLException) => throw e
            case (e: Throwable) => throw new SQLException(e.getMessage, e)
        }

    override def clearBatch(): Unit = batch.clear()

    override def cancel(): Unit = throw new SQLFeatureNotSupportedException(
        "Statement cancellation is not supported"
    )

    override def clearWarnings(): Unit = warnings.clear()

    override def close(): Unit = if( !isClosedStatus ) {
        Result.clear()
        clearBatch()
        clearWarnings()

        isClosedStatus = true
    }

    override def execute(sql: String): Boolean = try {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        if( Result.isActive ) {
            throw new SQLFeatureNotSupportedException(
                "Please close the current result set" +
                " before executing a new statement"
            )
        }

        if( !batch.isEmpty ) {
            throw new SQLFeatureNotSupportedException(
                "Please clear the batch before executing a new statement"
            )
        }

        val stmts: List[SqlStatement] =
            connection.processor.parser.parseSqlStatements(sql)
        if( stmts.isEmpty )
            throw new SQLDataException(
                "Input does not contain a SQL statement"
            )

        batch ++= stmts

        getMoreResults()
    } catch {
        case (e: SQLException) => throw e
        case (e: Throwable) => throw new SQLException(e.getMessage, e)
    }

    override def getMoreResults(): Boolean = try {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        Result.clear()

        batch.dequeue() match {
            case (qs: SqlRelQueryStatement) =>
                Result.set(
                    new StatementResultSet(planQuery(qs), this)
                )

            case (us: SqlUpdateStatement) =>
                Result.set(
                    connection.processor.handleUpdateStatement(us)
                )

            case (as: SqlAdminStatement) =>
                connection.processor.handleAdminStatement(as)
                Result.set(None)

            case (aqs: SqlAdminQueryStatement) =>
                Result.set(
                    new MetaDataResultSet(
                        connection.processor.handleAdminQueryStatement(aqs)
                    )
                )
        }

        Result.isQueryResult
    } catch {
        case (e: SQLException) => throw e
        case (e: Throwable) => throw new SQLException(e.getMessage, e)
    }

    override def getMoreResults(current: Int): Boolean = {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        if( current == CLOSE_CURRENT_RESULT ) getMoreResults()
        else throw new SQLFeatureNotSupportedException(
            "Multiple open results are not supported"
        )
    }

    override def getResultSet(): java.sql.ResultSet = {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        Result.resultSetOpt orNull
    }

    override def getUpdateCount(): Int = {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        Result.updateCountOpt getOrElse (-1)
    }

    override def execute(sql: String, autoGeneratedKeys: Int): Boolean =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def execute(sql: String, columnIndexes: Array[Int]): Boolean =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def execute(sql: String, columnNames: Array[String]): Boolean =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def executeBatch(): Array[Int] = try {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        batch.toArray.map {
            case (_: SqlRelQueryStatement) =>
                throw new BatchUpdateException(
                    new SQLDataException(
                        "Found a query statement in update statement batch"
                    )
                )

            case (us: SqlUpdateStatement) =>
                connection.processor.handleUpdateStatement(us) getOrElse {
                    SUCCESS_NO_INFO
                }

            case (as: SqlAdminStatement) =>
                connection.processor.handleAdminStatement(as)
                SUCCESS_NO_INFO

            case (_: SqlAdminQueryStatement) =>
                throw new BatchUpdateException(
                    new SQLDataException(
                        "Found an admin query statement " +
                        "in update statement batch"
                    )
                )
        }
    } catch {
        case (e: SQLException) => throw e
        case (e: Throwable) => throw new SQLException(e.getMessage, e)
    }

    override def executeQuery(sql: String): java.sql.ResultSet = try {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        if( Result.isActive ) {
            throw new SQLFeatureNotSupportedException(
                "Please close the current result set" +
                " before executing a new statement"
            )
        }

        if( !batch.isEmpty ) {
            throw new SQLFeatureNotSupportedException(
                "Please clear the batch before executing a new statement"
            )
        }

        connection.processor.parser.parseSqlStatements(sql) match {
            case List(qs: SqlRelQueryStatement) =>
                val plan: RelEvalPlan = planQuery(qs)
                Result.set(new StatementResultSet(plan, this))

            case List(_) =>
                throw new SQLDataException(
                    "Input does not contain a SQL query statement"
                )

            case List() =>
                throw new SQLDataException(
                    "Input does not contain a SQL statement"
                )

            case _ =>
                throw new SQLDataException(
                    "Input contains multiple SQL statements"
                )
        }

        getResultSet()
    } catch {
        case (e: SQLException) => throw e
        case (e: Throwable) => throw new SQLException(e.getMessage, e)
    }

    override def executeUpdate(sql: String): Int = try {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        if( Result.isActive ) {
            throw new SQLFeatureNotSupportedException(
                "Please close the current result set" +
                " before executing a new statement"
            )
        }

        if( !batch.isEmpty ) {
            throw new SQLFeatureNotSupportedException(
                "Please clear the batch before executing a new statement"
            )
        }

        connection.processor.parser.parseSqlStatements(sql) match {
            case List(us: SqlUpdateStatement) =>
                val updateCountOpt: Option[Int] =
                    connection.processor.handleUpdateStatement(us)
                Result.set(updateCountOpt)

            case List(as: SqlAdminStatement) =>
                connection.processor.handleAdminStatement(as)
                Result.set(None)

            case List(_) =>
                throw new SQLDataException(
                    "Input does not contain a SQL update statement"
                )

            case List() =>
                throw new SQLDataException(
                    "Input does not contain a SQL statement"
                )

            case _ =>
                throw new SQLDataException(
                    "Input contains multiple SQL statements"
                )
        }

        getUpdateCount()
    } catch {
        case (e: SQLException) => throw e
        case (e: Throwable) => throw new SQLException(e.getMessage, e)
    }

    override def executeUpdate(sql: String, autoGeneratedKeys: Int): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def executeUpdate(sql: String, columnIndexes: Array[Int]): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def executeUpdate(sql: String, columnNames: Array[String]): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def getConnection(): java.sql.Connection = connection

    override def getFetchDirection(): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else FETCH_FORWARD
    override def getFetchSize(): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else 0

    override def getGeneratedKeys(): java.sql.ResultSet =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def getMaxFieldSize(): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else SqlType.maxVarCharLen

    override def getMaxRows(): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else maxRows

    override def getQueryTimeout(): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else 0

    override def getResultSetConcurrency(): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else CONCUR_READ_ONLY

    override def getResultSetHoldability(): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else CLOSE_CURSORS_AT_COMMIT

    override def getResultSetType(): Int =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else TYPE_FORWARD_ONLY

    override def getWarnings(): SQLWarning =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else warnings.headOption orNull

    override def isClosed(): Boolean = isClosedStatus

    override def isPoolable(): Boolean =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else false

    override def setCursorName(name: String): Unit =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else throw new SQLFeatureNotSupportedException(
            "Cursor name is not supported"
        )

    override def setEscapeProcessing(enable: Boolean): Unit =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else if( enable ) throw new SQLFeatureNotSupportedException(
            "Escape processing is not supported"
        )

    override def setFetchDirection(direction: Int): Unit =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else if( direction != getFetchDirection() ) {
            throw new SQLFeatureNotSupportedException(
                "Only \"FETCH_FORWARD\" direction is supported"
            )
        }

    override def setFetchSize(rows: Int): Unit =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else if( rows != getFetchSize ) {
            throw new SQLFeatureNotSupportedException(
                "Fetch size cannot be changed"
            )
        }

    override def setMaxFieldSize(max: Int): Unit =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else if( max != getMaxFieldSize() ) {
            throw new SQLFeatureNotSupportedException(
                "Max fields size cannot be changed"
            )
        }

    override def setMaxRows(max: Int): Unit =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else if( max < 0 ) throw new SQLDataException("Invalid value: " + max)
        else { maxRows = max }

    override def setPoolable(poolable: Boolean): Unit =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else if( poolable != isPoolable() ) {
            throw new SQLFeatureNotSupportedException(
                "Poolable rows cannot be changed"
            )
        }

    override def setQueryTimeout(seconds: Int): Unit =
        if( isClosed() ) throw new SQLException("Statement closed", "26000")
        else if( seconds != getQueryTimeout() ) {
            throw new SQLFeatureNotSupportedException(
                "Query timeout is not supported"
            )
        }

    override def closeOnCompletion(): Unit =
        throw new SQLFeatureNotSupportedException(
            "Close on completion request is not supported"
        )

    override def isCloseOnCompletion(): Boolean = false

    override def isWrapperFor(iface: java.lang.Class[_]) = false
    override def unwrap[T](iface: java.lang.Class[T]): T =
        throw new SQLDataException("Not a wrapper")
}
