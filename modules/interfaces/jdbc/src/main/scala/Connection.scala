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

import java.util.Properties
import java.util.concurrent.Executor

import java.sql.{SQLWarning, SQLException, SQLDataException}
import java.sql.{SQLClientInfoException, SQLFeatureNotSupportedException}
import java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT
import java.sql.Statement.NO_GENERATED_KEYS

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import com.scleradb.exec.Processor

class Connection(
    driver: java.sql.Driver,
    url: String,
    info: Properties
) extends java.sql.Connection {
    private val warnings: mutable.Queue[SQLWarning] = mutable.Queue()

    val processor = Processor(info)
    try processor.init() catch { case (e: SQLWarning) => warnings += e }

    private var isClosedStatus: Boolean = false

    override def close(): Unit =
        if( !isClosedStatus ) {
            processor.close()
            isClosedStatus = true
        }

    override def isClosed(): Boolean = isClosedStatus
    override def isValid(timeout: Int): Boolean = !isClosed()

    private var isReadOnlyStatus: Boolean = false
    override def setReadOnly(isReadOnly: Boolean): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        isReadOnlyStatus = isReadOnly
    override def isReadOnly(): Boolean =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else isReadOnlyStatus

    private def createStatement(isReadOnly: Boolean): java.sql.Statement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else new Statement(isReadOnly, this)

    private def prepareStatement(
        sql: String,
        isReadOnly: Boolean
    ): java.sql.PreparedStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else {
            // are the statement parameters indexed (marked as "$1", "$2", ...)
            // or not (marked as "?")
            val isStmtParamIndexed: Boolean =
                Option(info.getProperty("statementParamsFormat")) match {
                    case Some("indexed") => true
                    case _ => false
                }

            new PreparedStatement(sql, isReadOnly, isStmtParamIndexed, this)
        }
        
    private def prepareCall(
        sql: String,
        isReadOnly: Boolean
    ): java.sql.CallableStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Callable statements are not supported"
        )

    override def createStatement(): java.sql.Statement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else createStatement(isReadOnlyStatus)

    override def prepareStatement(sql: String): java.sql.PreparedStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else prepareStatement(sql, isReadOnlyStatus)
        
    override def prepareCall(sql: String): java.sql.CallableStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else prepareCall(sql, isReadOnlyStatus)

    override def nativeSQL(sql: String): String =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else sql

    override def setAutoCommit(isAutoCommit: Boolean): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( !isAutoCommit ) {
            throw new SQLFeatureNotSupportedException(
                "Transactions are not supported"
            )
        }

    override def getAutoCommit(): Boolean =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else true // transactions are not supported

    override def commit(): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Transactions are not supported"
        )

    override def rollback(): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Transactions are not supported"
        )

    override def getMetaData(): java.sql.DatabaseMetaData =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else new DatabaseMetaData(url, this)

    override def setCatalog(catalog: String): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")

    override def getCatalog(): String =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else null.asInstanceOf[String]

    override def setTransactionIsolation(level: Int): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Transactions are not supported"
        )

    override def getTransactionIsolation(): Int =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else java.sql.Connection.TRANSACTION_NONE

    override def getWarnings(): SQLWarning =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else warnings.headOption orNull

    override def clearWarnings(): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else warnings.clear()

    override def createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int
    ): java.sql.Statement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( resultSetType != java.sql.ResultSet.TYPE_FORWARD_ONLY )
            throw new SQLFeatureNotSupportedException(
                "Only FORWARD_ONLY result type is supported"
            )
        else createStatement(
            resultSetConcurrency == java.sql.ResultSet.CONCUR_READ_ONLY
        )

    override def prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int
    ): java.sql.PreparedStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( resultSetType != java.sql.ResultSet.TYPE_FORWARD_ONLY )
            throw new SQLFeatureNotSupportedException(
                "Only FORWARD_ONLY result type is supported"
            )
        else prepareStatement(
            sql, (resultSetConcurrency == java.sql.ResultSet.CONCUR_READ_ONLY)
        )
        
    override def prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int
    ): java.sql.CallableStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( resultSetType != java.sql.ResultSet.TYPE_FORWARD_ONLY )
            throw new SQLFeatureNotSupportedException(
                "ResultSetType: Only FORWARD_ONLY is supported"
            )
        else prepareCall(
            sql, (resultSetConcurrency == java.sql.ResultSet.CONCUR_READ_ONLY)
        )

    override def setTypeMap(
        map: java.util.Map[String, java.lang.Class[_]]
    ): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Type maps are not supported"
        )

    override def getTypeMap(): java.util.Map[String, java.lang.Class[_]] = 
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Type maps are not supported"
        )

    override def setHoldability(holdability: Int): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( holdability != getHoldability() )
            throw new SQLFeatureNotSupportedException(
                "Holdability: Only CLOSE_CURSORS_AT_COMMIT is supported"
            )
        else if( isClosed() )
            throw new SQLException("Connection closed", "08003")

    override def getHoldability(): Int =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else CLOSE_CURSORS_AT_COMMIT

    override def setSavepoint(): java.sql.Savepoint =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "SavePoints are not supported"
        )

    override def setSavepoint(name: String): java.sql.Savepoint =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "SavePoints are not supported"
        )

    override def rollback(savePoint: java.sql.Savepoint): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "SavePoints are not supported"
        )

    override def releaseSavepoint(savePoint: java.sql.Savepoint): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "SavePoints are not supported"
        )

    override def createStatement(
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): java.sql.Statement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( resultSetHoldability != getHoldability() )
            throw new SQLFeatureNotSupportedException(
                "Holdability: Only CLOSE_CURSORS_AT_COMMIT is supported"
            )
        else createStatement(resultSetType, resultSetConcurrency)

    override def prepareStatement(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): java.sql.PreparedStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( resultSetHoldability != getHoldability() )
            throw new SQLFeatureNotSupportedException(
                "Holdability: Only CLOSE_CURSORS_AT_COMMIT is supported"
            )
        else prepareStatement(sql, resultSetType, resultSetConcurrency)
        
    override def prepareCall(
        sql: String,
        resultSetType: Int,
        resultSetConcurrency: Int,
        resultSetHoldability: Int
    ): java.sql.CallableStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( resultSetHoldability != getHoldability() )
            throw new SQLFeatureNotSupportedException(
                "Holdability: Only CLOSE_CURSORS_AT_COMMIT is supported"
            )
        else prepareCall(sql, resultSetType, resultSetConcurrency)

    override def prepareStatement(
        sql: String,
        autoGeneratedKeys: Int
    ): java.sql.PreparedStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else if( autoGeneratedKeys != NO_GENERATED_KEYS )
            throw new SQLFeatureNotSupportedException(
                "AutogeneratedKeys: Only NO_GENERATED_KEYS is supported"
            )
        else prepareStatement(sql)

    override def prepareStatement(
        sql: String,
        columnIndexes: Array[Int]
    ): java.sql.PreparedStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def prepareStatement(
        sql: String,
        columnNames: Array[String]
    ): java.sql.PreparedStatement =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Autogenerated keys are not supported"
        )

    override def createClob(): java.sql.Clob =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "CLOB is not supported"
        )

    override def createBlob(): java.sql.Blob =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "BLOB is not supported"
        )

    override def createNClob(): java.sql.NClob =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "NCLOB is not supported"
        )

    override def createSQLXML(): java.sql.SQLXML =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "SQLXML is not supported"
        )

    private val clientInfo: Properties =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else new Properties(info)

    override def setClientInfo(name: String, value: String): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else try clientInfo.setProperty(name, value) catch {
            case (e: Throwable) =>
                val failures: Map[String, java.sql.ClientInfoStatus] =
                    Map(name -> java.sql.ClientInfoStatus.REASON_UNKNOWN)
                throw new SQLClientInfoException(failures.asJava, e)
        }

    override def setClientInfo(properties: Properties): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else properties.asScala.foreach { case (name, value) =>
            setClientInfo(name.toString, value.toString)
        }

    override def getClientInfo(name: String): String =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else try clientInfo.getProperty(name) catch {
            case (e: Throwable) =>
                val failures: Map[String, java.sql.ClientInfoStatus] = Map(
                    name -> java.sql.ClientInfoStatus.REASON_UNKNOWN_PROPERTY
                )

                throw new SQLClientInfoException(failures.asJava, e)
        }

    override def getClientInfo(): Properties =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else clientInfo

    override def createArrayOf(
        typeName: String,
        elements: Array[Object]
    ): java.sql.Array = 
        throw new SQLFeatureNotSupportedException("Array is not supported")

    override def createStruct(
        typeName: String,
        attributes: Array[Object]
    ): java.sql.Struct = 
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "Struct is not supported"
        )

    // for JDK 1.7
    def setSchema(schema: String): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")

    // for JDK 1.7
    def getSchema(): String =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else null.asInstanceOf[String]

    // for JDK 1.7
    def abort(executor: Executor): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else close()

    // for JDK 1.7
    def setNetworkTimeout(executor: Executor, milliseconds: Int): Unit =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "NetworkTimeout is not supported"
        )

    // for JDK 1.7
    def getNetworkTimeout(): Int =
        if( isClosed() ) throw new SQLException("Connection closed", "08003")
        else throw new SQLFeatureNotSupportedException(
            "NetworkTimeout is not supported"
        )

    override def isWrapperFor(iface: java.lang.Class[_]): Boolean = false
    override def unwrap[T](iface: java.lang.Class[T]): T =
        throw new SQLDataException("Not a wrapper")
}
