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

import java.sql.{SQLFeatureNotSupportedException, SQLDataException}
import java.sql.ResultSetMetaData.{columnNullable, columnNoNulls}

import com.scleradb.sql.types._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableResult

class ResultSetMetaData(
    columns: List[Column]
) extends java.sql.ResultSetMetaData {
    private def column(columnIndex: Int): Column =
        if( columnIndex < 1 || columnIndex > columns.size )
            throw new SQLFeatureNotSupportedException(
                "Column index (" + columnIndex + ") is out of bounds"
            )
        else columns(columnIndex-1)

    override def getCatalogName(columnIndex: Int): String = "" // not applicable
    override def getSchemaName(columnIndex: Int): String = "" // not applicable
    override def getTableName(columnIndex: Int): String = "" // not applicable

    override def getColumnClassName(columnIndex: Int): String =
        column(columnIndex).sqlType.javaClass.getName()
        
    override def getColumnCount(): Int = columns.size

    override def getColumnDisplaySize(columnIndex: Int): Int =
        SqlType.displaySize(column(columnIndex).sqlType)

    override def getColumnLabel(columnIndex: Int): String =
        getColumnName(columnIndex)
        
    override def getColumnName(columnIndex: Int): String =
        column(columnIndex).name
        
    override def getColumnType(columnIndex: Int): Int =
        column(columnIndex).sqlType.sqlTypeCode
        
    override def getColumnTypeName(columnIndex: Int): String =
        column(columnIndex).sqlType.repr
        
    override def getPrecision(columnIndex: Int): Int =
        SqlType.precisionOpt(column(columnIndex).sqlType) getOrElse 0
        
    override def getScale(columnIndex: Int): Int =
        SqlType.scaleOpt(column(columnIndex).sqlType) getOrElse 0

    override def isAutoIncrement(columnIndex: Int): Boolean = false

    override def isCaseSensitive(columnIndex: Int): Boolean =
        column(columnIndex).sqlType match {
            case (_: SqlChar) => true
            case _ => false
        }

    override def isCurrency(columnIndex: Int): Boolean = false

    override def isDefinitelyWritable(columnIndex: Int): Boolean = false

    override def isNullable(columnIndex: Int): Int =
        if( column(columnIndex).sqlType.isOption ) columnNullable
        else columnNoNulls

    override def isReadOnly(columnIndex: Int): Boolean = true

    override def isSearchable(columnIndex: Int): Boolean = true

    override def isSigned(columnIndex: Int): Boolean = 
        column(columnIndex).sqlType match {
            case SqlBigInt | SqlDecimal(_, _) | SqlFloat(_) |
                 SqlInteger | SqlReal | SqlSmallInt => true
            case _ => false
        }

    override def isWritable(columnIndex: Int): Boolean = false

    override def isWrapperFor(iface: java.lang.Class[_]) = false
    override def unwrap[T](iface: java.lang.Class[T]): T =
        throw new SQLDataException("Not a wrapper")
}
