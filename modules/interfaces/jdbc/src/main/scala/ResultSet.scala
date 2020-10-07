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

import java.sql.{SQLFeatureNotSupportedException, SQLDataException}
import java.sql.{SQLWarning, SQLException}

import java.sql.ResultSet.{TYPE_FORWARD_ONLY, CONCUR_READ_ONLY, FETCH_FORWARD}
import java.sql.ResultSet.CLOSE_CURSORS_AT_COMMIT

import scala.collection.mutable

import com.scleradb.sql.expr._
import com.scleradb.sql.types._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, TableRow}
import com.scleradb.sql.plan.RelEvalPlan

sealed abstract class ResultSet extends java.sql.ResultSet {
    protected val tableResult: TableResult

    private var isClosedStatus: Boolean = false
    private val warnings: mutable.Queue[SQLWarning] = mutable.Queue()

    private object Rows {
        private val iter: Iterator[TableRow] = tableResult.rows
        private var rowOpt: Option[TableRow] = None
        private var rowCount: Int = 0
        private var wasNullFlag: Boolean = false

        private def nextRowOpt: Option[TableRow] =
            try {
                if( iter.hasNext ) {
                    rowCount += 1
                    Some(iter.next())
                } else None
            } catch {
                case (e: SQLException) => throw e
                case (e: Throwable) => throw new SQLException(e.getMessage, e)
            }

        def advanceRow(): Unit = { rowOpt = nextRowOpt }

        def isValidRow: Boolean = (rowOpt != None)
        def isFirstRow: Boolean = (rowCount == 1)
        def isLastRow: Boolean = isValidRow && !iter.hasNext

        def rowNumber: Int = rowCount
        def currentRow: TableRow = rowOpt match {
            case Some(row) => row
            case None => throw new SQLDataException("ResultSet is empty")
        }

        private def valueOrNull[T](vOpt: Option[T]): T = {
            wasNullFlag = false
            vOpt getOrElse { wasNullFlag = true; null.asInstanceOf[T] }
        }

        def wasNull: Boolean = wasNullFlag

        def getObject(col: Column): Object = valueOrNull {
            currentRow.getScalExpr(col) match {
                case (e: ScalValueBase) => Some(e.value.asInstanceOf[Object])
                case (_: SqlNull) => None
            }
        }

        def getString(col: Column): String = valueOrNull {
            currentRow.getStringOpt(col.name)
        }

        def getInt(col: Column): Int = valueOrNull {
            currentRow.getIntOpt(col.name)
        }

        def getShort(col: Column): Short = valueOrNull {
            currentRow.getShortOpt(col.name)
        }

        def getLong(col: Column): Long = valueOrNull {
            currentRow.getLongOpt(col.name)
        }

        def getFloat(col: Column): Float = valueOrNull {
            currentRow.getFloatOpt(col.name)
        }

        def getDouble(col: Column): Double = valueOrNull {
            currentRow.getDoubleOpt(col.name)
        }

        def getBoolean(col: Column): Boolean = valueOrNull {
            currentRow.getBooleanOpt(col.name)
        }

        def getDate(col: Column): java.sql.Date = valueOrNull {
            currentRow.getDateOpt(col.name)
        }

        def getTime(col: Column): java.sql.Time = valueOrNull {
            currentRow.getTimeOpt(col.name)
        }

        def getTimestamp(col: Column): java.sql.Timestamp = valueOrNull {
            currentRow.getTimestampOpt(col.name)
        }
    }

    private def column(columnLabel: String): Column =
        tableResult.columnOpt(columnLabel) getOrElse {
            throw new SQLDataException(
                "Column \"" + columnLabel + "\" not found"
            )
        }

    private def column(columnIndex: Int): Column =
        if( columnIndex < 1 || columnIndex > tableResult.columns.size )
            throw new SQLDataException(
                "Column index (" + columnIndex + ") is out of bounds"
            )
        else tableResult.columns(columnIndex-1)

    override def absolute(row: Int): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"absolute\" is not supported"
        )

    override def afterLast(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"afterLast\" is not supported"
        )

    override def beforeFirst(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"beforeFirst\" is not supported"
        )

    override def cancelRowUpdates(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")

    override def clearWarnings(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else warnings.clear()

    override def close(): Unit = {
        clearWarnings()
        isClosedStatus = true
    }

    override def deleteRow(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"deleteRow\" is not supported"
        )

    override def findColumn(columnLabel: String): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else tableResult.columns.indexOf(column(columnLabel)) + 1

    override def first(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"first\" is not supported"
        )

    override def getArray(columnIndex: Int): java.sql.Array =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getArray\" is not supported"
        )

    override def getArray(columnLabel: String): java.sql.Array =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getArray\" is not supported"
        )

    override def getAsciiStream(columnIndex: Int): java.io.InputStream =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getAsciiStream\" is not supported"
        )

    override def getAsciiStream(columnLabel: String): java.io.InputStream =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getAsciiStream\" is not supported"
        )

    override def getBigDecimal(columnIndex: Int): java.math.BigDecimal =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBigDecimal\" is not supported"
        )

    override def getBigDecimal(
        columnIndex: Int,
        scale: Int
    ): java.math.BigDecimal =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBigDecimal\" is not supported"
        )

    override def getBigDecimal(columnLabel: String): java.math.BigDecimal =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBigDecimal\" is not supported"
        )

    override def getBigDecimal(
        columnLabel: String,
        scale: Int
    ): java.math.BigDecimal =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBigDecimal\" is not supported"
        )

    override def getBinaryStream(columnIndex: Int): java.io.InputStream =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBinaryStream\" is not supported"
        )

    override def getBinaryStream(columnLabel: String): java.io.InputStream =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBinaryStream\" is not supported"
        )

    override def getBlob(columnIndex: Int): java.sql.Blob =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBlob\" is not supported"
        )

    override def getBlob(columnLabel: String): java.sql.Blob =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBlob\" is not supported"
        )

    override def getBoolean(columnIndex: Int): Boolean=
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getBoolean(column(columnIndex))

    override def getBoolean(columnLabel: String): Boolean=
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getBoolean(column(columnLabel))

    override def getByte(columnIndex: Int): Byte =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getByte\" is not supported"
        )

    override def getByte(columnLabel: String): Byte =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getByte\" is not supported"
        )

    override def getBytes(columnIndex: Int): Array[Byte] =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBytes\" is not supported"
        )

    override def getBytes(columnLabel: String): Array[Byte] =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getBytes\" is not supported"
        )

    override def getCharacterStream(columnIndex: Int): java.io.Reader =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getCharacterStream\" is not supported"
        )

    override def getCharacterStream(columnLabel: String): java.io.Reader =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getCharacterStream\" is not supported"
        )

    override def getClob(columnIndex: Int): java.sql.Clob =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getClob\" is not supported"
        )

    override def getClob(columnLabel: String): java.sql.Clob =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getClob\" is not supported"
        )

    override def getConcurrency(): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else CONCUR_READ_ONLY

    override def getCursorName(): String =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getCursorName\" is not supported"
        )

    override def getDate(columnIndex: Int): java.sql.Date =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getDate(column(columnIndex))

    override def getDate(
        columnIndex: Int,
        cal: java.util.Calendar
    ): java.sql.Date =
        Rows.getDate(column(columnIndex))

    override def getDate(columnLabel: String): java.sql.Date =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getDate(column(columnLabel))

    override def getDate(
        columnLabel: String,
        cal: java.util.Calendar
    ): java.sql.Date =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getDate(column(columnLabel))

    override def getDouble(columnIndex: Int): Double =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getDouble(column(columnIndex))

    override def getDouble(columnLabel: String): Double =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getDouble(column(columnLabel))

    override def getFetchDirection(): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else FETCH_FORWARD

    override def getFetchSize(): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else 0

    override def getFloat(columnIndex: Int): Float =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getFloat(column(columnIndex))

    override def getFloat(columnLabel: String): Float =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getFloat(column(columnLabel))

    override def getHoldability(): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else CLOSE_CURSORS_AT_COMMIT

    override def getInt(columnIndex: Int): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getInt(column(columnIndex))

    override def getInt(columnLabel: String): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getInt(column(columnLabel))

    override def getLong(columnIndex: Int): Long =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getLong(column(columnIndex))

    override def getLong(columnLabel: String): Long =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getLong(column(columnLabel))

    override def getMetaData(): java.sql.ResultSetMetaData =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else new ResultSetMetaData(tableResult.columns)

    override def getNCharacterStream(columnIndex: Int): java.io.Reader =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getNCharacterStream\" is not supported"
        )

    override def getNCharacterStream(columnLabel: String): java.io.Reader =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getNCharacterStream\" is not supported"
        )

    override def getNClob(columnIndex: Int): java.sql.NClob =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getNClob\" is not supported"
        )

    override def getNClob(columnLabel: String): java.sql.NClob =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getNClob\" is not supported"
        )

    override def getNString(columnIndex: Int): String =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getNString\" is not supported"
        )

    override def getNString(columnLabel: String): String =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getNString\" is not supported"
        )

    override def getObject(columnIndex: Int): Object =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getObject(column(columnIndex))

    override def getObject[T](columnIndex: Int, t: java.lang.Class[T]): T =
        getObject(columnIndex).asInstanceOf[T]

    override def getObject(columnLabel: String): Object =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getObject(column(columnLabel))

    override def getObject[T](columnLabel: String, t: java.lang.Class[T]): T =
        getObject(columnLabel).asInstanceOf[T]

    override def getObject(
        columnIndex: Int,
        map: java.util.Map[String, java.lang.Class[_]]
    ): Object =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getObject(column(columnIndex))

    override def getObject(
        columnLabel: String,
        map: java.util.Map[String, java.lang.Class[_]]
    ): Object =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getObject(column(columnLabel))

    override def getRef(columnIndex: Int): java.sql.Ref =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getRef\" is not supported"
        )

    override def getRef(columnLabel: String): java.sql.Ref =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getRef\" is not supported"
        )

    override def getRow(): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.rowNumber

    override def getRowId(columnIndex: Int): java.sql.RowId =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getRowId\" is not supported"
        )

    override def getRowId(columnLabel: String): java.sql.RowId =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getRowId\" is not supported"
        )

    override def getShort(columnIndex: Int): Short =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getShort(column(columnIndex))

    override def getShort(columnLabel: String): Short =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getShort(column(columnLabel))

    override def getSQLXML(columnIndex: Int): java.sql.SQLXML =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getSQLXML\" is not supported"
        )

    override def getSQLXML(columnLabel: String): java.sql.SQLXML =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getSQLXML\" is not supported"
        )

    override def getString(columnIndex: Int): String =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getString(column(columnIndex))

    override def getString(columnLabel: String): String =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getString(column(columnLabel))

    override def getTime(columnIndex: Int): java.sql.Time =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getTime(column(columnIndex))

    override def getTime(
        columnIndex: Int,
        cal: java.util.Calendar
    ): java.sql.Time =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getTime(column(columnIndex))

    override def getTime(columnLabel: String): java.sql.Time =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getTime(column(columnLabel))

    override def getTime(
        columnLabel: String,
        cal: java.util.Calendar
    ): java.sql.Time =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getTime(column(columnLabel))

    override def getTimestamp(columnIndex: Int): java.sql.Timestamp =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getTimestamp(column(columnIndex))

    override def getTimestamp(
        columnIndex: Int,
        cal: java.util.Calendar
    ): java.sql.Timestamp =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getTimestamp(column(columnIndex))

    override def getTimestamp(columnLabel: String): java.sql.Timestamp =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getTimestamp(column(columnLabel))

    override def getTimestamp(
        columnLabel: String,
        cal: java.util.Calendar
    ): java.sql.Timestamp =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.getTimestamp(column(columnLabel))

    override def getType(): Int =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else TYPE_FORWARD_ONLY

    override def getUnicodeStream(columnIndex: Int): java.io.InputStream =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getUnicodeStream\" is not supported"
        )

    override def getUnicodeStream(columnLabel: String): java.io.InputStream =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getUnicodeStream\" is not supported"
        )

    override def getURL(columnIndex: Int): java.net.URL =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getURL\" is not supported"
        )

    override def getURL(columnLabel: String): java.net.URL =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"getURL\" is not supported"
        )

    override def getWarnings(): SQLWarning =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else warnings.headOption orNull

    override def insertRow(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"insertRow\" is not supported"
        )

    override def isAfterLast(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"isAfterLast\" is not supported"
        )

    override def isBeforeFirst(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"isBeforeFirst\" is not supported"
        )

    override def isClosed(): Boolean = isClosedStatus

    override def isFirst(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.isFirstRow

    override def isLast(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else Rows.isLastRow

    override def last(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"last\" is not supported"
        )

    override def moveToCurrentRow(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"moveToCurrentRow\" is not supported"
        )

    override def moveToInsertRow(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"moveToInsertRow\" is not supported"
        )

    override def next(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed") else {
            Rows.advanceRow()
            Rows.isValidRow
        }

    override def previous(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"previous\" is not supported"
        )

    override def refreshRow(): Unit = {
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"refreshRow\" is not supported"
        )
    }

    override def relative(row: Int): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"relative\" is not supported"
        )

    override def rowDeleted(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"rowDeleted\" is not supported"
        )

    override def rowInserted(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"rowInserted\" is not supported"
        )

    override def rowUpdated(): Boolean =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"rowUpdated\" is not supported"
        )

    override def setFetchDirection(direction: Int): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else if( direction != getFetchDirection() )
            throw new SQLFeatureNotSupportedException(
                "Only \"FETCH_FORWARD\" direction is supported"
            )

    override def setFetchSize(rows: Int): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else if( rows != getFetchSize ) {
            throw new SQLFeatureNotSupportedException(
                "Fetch size cannot be changed"
            )
        }

    override def updateArray(columnIndex: Int, x: java.sql.Array): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateArray\" is not supported"
        )

    override def updateArray(columnLabel: String, x: java.sql.Array): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateArray\" is not supported"
        )

    override def updateAsciiStream(
        columnIndex: Int,
        x: java.io.InputStream
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateAsciiStream\" is not supported"
        )

    override def updateAsciiStream(
        columnIndex: Int,
        x: java.io.InputStream,
        length: Int
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateAsciiStream\" is not supported"
        )

    override def updateAsciiStream(
        columnIndex: Int,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateAsciiStream\" is not supported"
        )

    override def updateAsciiStream(
        columnLabel: String,
        x: java.io.InputStream
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateAsciiStream\" is not supported"
        )

    override def updateAsciiStream(
        columnLabel: String,
        x: java.io.InputStream,
        length: Int
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateAsciiStream\" is not supported"
        )

    override def updateAsciiStream(
        columnLabel: String,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateAsciiStream\" is not supported"
        )

    override def updateBigDecimal(
        columnIndex: Int,
        x: java.math.BigDecimal
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBigDecimal\" is not supported"
        )

    override def updateBigDecimal(
        columnLabel: String,
        x: java.math.BigDecimal
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBigDecimal\" is not supported"
        )

    override def updateBinaryStream(
        columnIndex: Int,
        x: java.io.InputStream
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBinaryStream\" is not supported"
        )

    override def updateBinaryStream(
        columnIndex: Int,
        x: java.io.InputStream,
        length: Int
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBinaryStream\" is not supported"
        )

    override def updateBinaryStream(
        columnIndex: Int,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBinaryStream\" is not supported"
        )

    override def updateBinaryStream(
        columnLabel: String,
        x: java.io.InputStream
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBinaryStream\" is not supported"
        )

    override def updateBinaryStream(
        columnLabel: String,
        x: java.io.InputStream,
        length: Int
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBinaryStream\" is not supported"
        )

    override def updateBinaryStream(
        columnLabel: String,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBinaryStream\" is not supported"
        )

    override def updateBlob(
        columnIndex: Int,
        x: java.sql.Blob
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBlob\" is not supported"
        )

    override def updateBlob(
        columnIndex: Int,
        x: java.io.InputStream
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBlob\" is not supported"
        )

    override def updateBlob(
        columnIndex: Int,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBlob\" is not supported"
        )

    override def updateBlob(
        columnLabel: String,
        x: java.sql.Blob
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBlob\" is not supported"
        )

    override def updateBlob(
        columnLabel: String,
        x: java.io.InputStream
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBlob\" is not supported"
        )

    override def updateBlob(
        columnLabel: String,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBlob\" is not supported"
        )

    override def updateBoolean(columnIndex: Int, x: Boolean): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBoolean\" is not supported"
        )

    override def updateBoolean(columnLabel: String, x: Boolean): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBoolean\" is not supported"
        )

    override def updateByte(columnIndex: Int, x: Byte): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateByte\" is not supported"
        )

    override def updateByte(columnLabel: String, x: Byte): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateByte\" is not supported"
        )

    override def updateBytes(columnIndex: Int, x: Array[Byte]): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBytes\" is not supported"
        )

    override def updateBytes(columnLabel: String, x: Array[Byte]): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateBytes\" is not supported"
        )

    override def updateCharacterStream(
        columnIndex: Int,
        x: java.io.Reader
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateCharacterStream\" is not supported"
        )

    override def updateCharacterStream(
        columnIndex: Int,
        x: java.io.Reader,
        length: Int
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateCharacterStream\" is not supported"
        )

    override def updateCharacterStream(
        columnIndex: Int,
        x: java.io.Reader,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateCharacterStream\" is not supported"
        )

    override def updateCharacterStream(
        columnLabel: String,
        x: java.io.Reader
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateCharacterStream\" is not supported"
        )

    override def updateCharacterStream(
        columnLabel: String,
        x: java.io.Reader,
        length: Int
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateCharacterStream\" is not supported"
        )

    override def updateCharacterStream(
        columnLabel: String,
        x: java.io.Reader,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateCharacterStream\" is not supported"
        )

    override def updateClob(
        columnIndex: Int,
        x: java.sql.Clob
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateClob\" is not supported"
        )

    override def updateClob(
        columnIndex: Int,
        x: java.io.Reader
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateClob\" is not supported"
        )

    override def updateClob(
        columnIndex: Int,
        x: java.io.Reader,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateClob\" is not supported"
        )

    override def updateClob(
        columnLabel: String,
        x: java.sql.Clob
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateClob\" is not supported"
        )

    override def updateClob(
        columnLabel: String,
        x: java.io.Reader
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateClob\" is not supported"
        )

    override def updateClob(
        columnLabel: String,
        x: java.io.Reader,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateClob\" is not supported"
        )

    override def updateDate(columnIndex: Int, x: java.sql.Date): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateDate\" is not supported"
        )

    override def updateDate(columnLabel: String, x: java.sql.Date): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateDate\" is not supported"
        )

    override def updateDouble(columnIndex: Int, x: Double): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateDouble\" is not supported"
        )

    override def updateDouble(columnLabel: String, x: Double): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateDouble\" is not supported"
        )

    override def updateFloat(columnIndex: Int, x: Float): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateFloat\" is not supported"
        )

    override def updateFloat(columnLabel: String, x: Float): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateFloat\" is not supported"
        )

    override def updateInt(columnIndex: Int, x: Int): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateInt\" is not supported"
        )

    override def updateInt(columnLabel: String, x: Int): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateInt\" is not supported"
        )

    override def updateLong(columnIndex: Int, x: Long): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateLong\" is not supported"
        )

    override def updateLong(columnLabel: String, x: Long): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateLong\" is not supported"
        )

    override def updateNCharacterStream(
        columnIndex: Int,
        x: java.io.Reader
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNCharacterStream\" is not supported"
        )

    override def updateNCharacterStream(
        columnIndex: Int,
        x: java.io.Reader,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNCharacterStream\" is not supported"
        )

    override def updateNCharacterStream(
        columnLabel: String,
        x: java.io.Reader
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNCharacterStream\" is not supported"
        )

    override def updateNCharacterStream(
        columnLabel: String,
        x: java.io.Reader,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNCharacterStream\" is not supported"
        )

    override def updateNClob(
        columnIndex: Int,
        x: java.sql.NClob
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNClob\" is not supported"
        )

    override def updateNClob(
        columnIndex: Int,
        x: java.io.Reader
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNClob\" is not supported"
        )

    override def updateNClob(
        columnIndex: Int,
        x: java.io.Reader,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNClob\" is not supported"
        )

    override def updateNClob(
        columnLabel: String,
        x: java.sql.NClob
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNClob\" is not supported"
        )

    override def updateNClob(
        columnLabel: String,
        x: java.io.Reader
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNClob\" is not supported"
        )

    override def updateNClob(
        columnLabel: String,
        x: java.io.Reader,
        length: Long
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNClob\" is not supported"
        )

    override def updateNString(columnIndex: Int, x: String): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNString\" is not supported"
        )

    override def updateNString(columnLabel: String, x: String): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNString\" is not supported"
        )

    override def updateNull(columnIndex: Int): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNull\" is not supported"
        )

    override def updateNull(columnLabel: String): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateNull\" is not supported"
        )

    override def updateObject(columnIndex: Int, x: Object): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateObject\" is not supported"
        )

    override def updateObject(
        columnIndex: Int,
        x: Object,
        scaleOrLength: Int
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateObject\" is not supported"
        )

    override def updateObject(columnLabel: String, x: Object): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateObject\" is not supported"
        )

    override def updateObject(
        columnLabel: String,
        x: Object,
        scaleOrLength: Int
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateObject\" is not supported"
        )

    override def updateRef(columnIndex: Int, x: java.sql.Ref): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateRef\" is not supported"
        )

    override def updateRef(columnLabel: String, x: java.sql.Ref): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateRef\" is not supported"
        )

    override def updateRow(): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateRow\" is not supported"
        )

    override def updateRowId(columnIndex: Int, x: java.sql.RowId): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateRowId\" is not supported"
        )

    override def updateRowId(columnLabel: String, x: java.sql.RowId): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateRowId\" is not supported"
        )

    override def updateShort(columnIndex: Int, x: Short): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateShort\" is not supported"
        )

    override def updateShort(columnLabel: String, x: Short): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateShort\" is not supported"
        )

    override def updateSQLXML(columnIndex: Int, x: java.sql.SQLXML): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateSQLXML\" is not supported"
        )

    override def updateSQLXML(columnLabel: String, x: java.sql.SQLXML): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateSQLXML\" is not supported"
        )

    override def updateString(columnIndex: Int, x: String): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateString\" is not supported"
        )

    override def updateString(columnLabel: String, x: String): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateString\" is not supported"
        )

    override def updateTime(columnIndex: Int, x: java.sql.Time): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateTime\" is not supported"
        )

    override def updateTime(columnLabel: String, x: java.sql.Time): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateTime\" is not supported"
        )

    override def updateTimestamp(
        columnIndex: Int,
        x: java.sql.Timestamp
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateTimestamp\" is not supported"
        )

    override def updateTimestamp(
        columnLabel: String,
        x: java.sql.Timestamp
    ): Unit =
        if( isClosed() ) throw new SQLDataException("ResultSet closed")
        else throw new SQLFeatureNotSupportedException(
            "Function \"updateTimestamp\" is not supported"
        )

    override def wasNull(): Boolean = Rows.wasNull

    override def isWrapperFor(iface: java.lang.Class[_]) = false
    override def unwrap[T](iface: java.lang.Class[T]): T =
        throw new SQLDataException("Not a wrapper")
}

class MetaDataResultSet(
    override val tableResult: TableResult
) extends ResultSet {
    override def getStatement(): java.sql.Statement =
        null.asInstanceOf[java.sql.Statement]
}

class StatementResultSet(
    plan: RelEvalPlan,
    statement: java.sql.Statement
) extends ResultSet {
    plan.init()
    override val tableResult: TableResult = plan.result.tableResult

    override def getStatement(): java.sql.Statement = statement

    override def close(): Unit = if( !isClosed() ) {
        super.close()
        plan.dispose() 
    }
}
