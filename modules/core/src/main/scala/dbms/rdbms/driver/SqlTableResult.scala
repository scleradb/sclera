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

import java.sql.{Types, ResultSet, ResultSetMetaData, Statement}

import com.scleradb.sql.expr.SortExpr
import com.scleradb.sql.types.SqlType
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableResult

class SqlTableResult(
    statement: Statement,
    queryStr: String,
    override val resultOrder: List[SortExpr]
) extends TableResult {
    private val resultSet: ResultSet =
        try statement.executeQuery(queryStr) catch { case (e: Throwable) =>
            statement.close()
            throw e
        }

    private val metaData: ResultSetMetaData = resultSet.getMetaData()
    private val nColumns: Int = metaData.getColumnCount()

    val columns: List[Column] = (1 to nColumns).toList.map { colIndex => 
        val name: String = metaData.getColumnLabel(colIndex)
        val colType: Int = metaData.getColumnType(colIndex)
        if( colType == Types.ARRAY ) throw new RuntimeException(
            "Arrays not supported"
        )

        val len: Int = metaData.getColumnDisplaySize(colIndex)
        val lenOpt: Option[Int] =
            if( len >= 0 && len <= SqlType.maxLen ) Some(len)
            else None

        val readPrec: Int = metaData.getPrecision(colIndex)
        val prec: Int = if( readPrec < len - 1 ) (len - 1) else readPrec
        val precOpt: Option[Int] =
            if( prec >= 0 && prec <= SqlType.maxPrec ) Some(prec)
            else None

        val scale: Int = metaData.getScale(colIndex)
        val scaleOpt: Option[Int] =
            if( scale >= 0 && scale <= SqlType.maxScale ) Some(scale)
            else None

        val className: String = metaData.getColumnClassName(colIndex)

        val sqlType: SqlType =
            try SqlType(colType, lenOpt, precOpt, scaleOpt, Some(className))
            catch { case (e: Throwable) =>
                throw new IllegalArgumentException(
                    "Type of column \"" + name +
                    "\" is not supported: " + e.getMessage(),
                    e
                )
            }

        if( metaData.isNullable(colIndex) == ResultSetMetaData.columnNoNulls )
            Column(name.toUpperCase, sqlType)
        else Column(name.toUpperCase, sqlType.option)
    }

    override def rows: Iterator[SqlTableRow] = new Iterator[SqlTableRow] {
        // need var to avoid incrementing resultSet at each check
        var hasNext: Boolean = resultSet.next()
        def next(): SqlTableRow = if( hasNext ) {
            val valMap: Map[String, Option[java.lang.Object]] = Map() ++
                columns.map { col =>
                    val v: java.lang.Object = resultSet.getObject(col.name)
                    (col.name.toUpperCase ->
                        (if( resultSet.wasNull ) None else Some(v)))
                }

            hasNext = resultSet.next()
            SqlTableRow(valMap, columns)
        } else Iterator.empty.next()
    }

    override def close(): Unit = {
        resultSet.close()
        statement.close()
    }
}

object SqlTableResult {
    def apply(
        statement: Statement,
        queryStr: String,
        resultOrder: List[SortExpr]
    ): SqlTableResult = new SqlTableResult(statement, queryStr, resultOrder)
}
