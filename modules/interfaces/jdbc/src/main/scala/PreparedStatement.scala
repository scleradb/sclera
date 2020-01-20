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

import java.sql.SQLException
import java.sql.{SQLFeatureNotSupportedException, SQLDataException}

import scala.util.matching.Regex
import scala.collection.mutable

import com.scleradb.sql.types.SqlType
import com.scleradb.sql.expr._
import com.scleradb.sql.plan.RelEvalPlan
import com.scleradb.sql.statements.SqlRelQueryStatement
import com.scleradb.sql.statements.SqlAdminQueryStatement

class PreparedStatement(
    paramStmt: String,
    isReadOnlyStatus: Boolean,
    isStmtParamIndexed: Boolean,
    conn: Connection
) extends Statement(isReadOnlyStatus, conn) with java.sql.PreparedStatement {
    private val indexedParamStmt: String =
        if( isStmtParamIndexed ) paramStmt else {
            val parseRegex: Regex = """([^"'\\\?]+|"[^"]*"|'[^']*'|\\.|\?)""".r
            val parsedStatement: List[String] =
                parseRegex.findAllIn(paramStmt).toList
            val (replaced, _) = parsedStatement.foldLeft ("", 1) {
                case ((str, n), token) if token.trim == "?" =>
                    (str + token.replace("?", "$" + n), n + 1)
                case ((str, n), token) if token.trim == "\\?" =>
                    (str + token.replace("\\?", "?"), n)
                case ((str, n), token) =>
                    (str + token, n)
            }

            replaced
        }

    private val params: mutable.Map[Int, ScalExpr] = mutable.Map()

    private def statement: String = params.foldLeft (indexedParamStmt) {
        case (s, (n, e)) => """\$%d""".format(n).r.replaceAllIn(s, e.repr)
    }

    override def addBatch(): Unit = addBatch(statement)

    override def clearParameters(): Unit = params.clear()

    override def execute(): Boolean = execute(statement)
    override def executeQuery(): java.sql.ResultSet = executeQuery(statement)
    override def executeUpdate(): Int = executeUpdate(statement)

    override def getMetaData(): java.sql.ResultSetMetaData = try {
        if( isClosed() ) throw new SQLException("Statement closed", "26000")

        conn.processor.parser.parseSqlStatements(statement) match {
            case List(qs: SqlRelQueryStatement) =>
                val plan: RelEvalPlan = conn.processor.planQuery(qs, Some(0))

                val rs: java.sql.ResultSet = new StatementResultSet(plan, this)
                val meta: java.sql.ResultSetMetaData = rs.getMetaData()
                rs.close()

                meta

            case List(aqs: SqlAdminQueryStatement) =>
                val rs: java.sql.ResultSet =
                    new MetaDataResultSet(
                        conn.processor.handleAdminQueryStatement(aqs)
                    )

                val meta: java.sql.ResultSetMetaData = rs.getMetaData()
                rs.close()

                meta

            case List(_) =>
                null.asInstanceOf[java.sql.ResultSetMetaData]

            case List() =>
                throw new SQLDataException(
                    "Input does not contain a SQL statement"
                )

            case _ =>
                throw new SQLDataException(
                    "Input contains multiple SQL statements"
                )
        }
    } catch {
        case (e: SQLException) => throw e
        case (e: Throwable) => throw new SQLException(e.getMessage, e)
    }

    override def getParameterMetaData(): java.sql.ParameterMetaData = {
        val nParams: Int = """\$\d+""".r.findAllIn(indexedParamStmt).size
        new ParameterMetaData(nParams)
    }

    override def setArray(index: Int, x: java.sql.Array): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Array\" is not supported"
        )

    override def setAsciiStream(index: Int, x: java.io.InputStream): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"AsciiStream\" is not supported"
        )

    override def setAsciiStream(
        index: Int,
        x: java.io.InputStream,
        length: Int
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"AsciiStream\" is not supported"
        )

    override def setAsciiStream(
        index: Int,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"AsciiStream\" is not supported"
        )

    override def setBigDecimal(index: Int, x: java.math.BigDecimal): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"BigDecimal\" is not supported"
        )

    override def setBinaryStream(index: Int, x: java.io.InputStream): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"BinaryStream\" is not supported"
        )

    override def setBinaryStream(
        index: Int,
        x: java.io.InputStream,
        length: Int
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"BinaryStream\" is not supported"
        )

    override def setBinaryStream(
        index: Int,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"BinaryStream\" is not supported"
        )

    override def setBlob(index: Int, x: java.io.InputStream): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Blob\" is not supported"
        )

    override def setBlob(
        index: Int,
        x: java.io.InputStream,
        length: Long
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Blob\" is not supported"
        )

    override def setBlob(index: Int, x: java.sql.Blob): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Blob\" is not supported"
        )

    override def setBoolean(index: Int, x: Boolean): Unit = {
        params += (index -> BoolConst(x))
    }

    override def setByte(index: Int, x: Byte): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Byte\" is not supported"
        )

    override def setBytes(index: Int, x: Array[Byte]): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Bytes\" is not supported"
        )

    override def setCharacterStream(index: Int, x: java.io.Reader): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"CharacterStream\" is not supported"
        )

    override def setCharacterStream(
        index: Int,
        x: java.io.Reader,
        length: Int
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"CharacterStream\" is not supported"
        )

    override def setCharacterStream(
        index: Int,
        x: java.io.Reader,
        length: Long
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"CharacterStream\" is not supported"
        )

    override def setClob(index: Int, x: java.io.Reader): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Clob\" is not supported"
        )

    override def setClob(
        index: Int,
        x: java.io.Reader,
        length: Long
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Clob\" is not supported"
        )

    override def setClob(index: Int, x: java.sql.Clob): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"Clob\" is not supported"
        )

    override def setDate(index: Int, x: java.sql.Date): Unit = {
        params += (index -> DateConst(x))
    }

    override def setDate(
        index: Int,
        x: java.sql.Date,
        cal: java.util.Calendar
    ): Unit = {
        params += (index -> DateConst(x))
    }

    override def setDouble(index: Int, x: Double): Unit = {
        params += (index -> DoubleConst(x))
    }

    override def setFloat(index: Int, x: Float): Unit = {
        params += (index -> FloatConst(x))
    }

    override def setInt(index: Int, x: Int): Unit = {
        params += (index -> IntConst(x))
    }

    override def setLong(index: Int, x: Long): Unit = {
        params += (index -> LongConst(x))
    }

    override def setNCharacterStream(index: Int, x: java.io.Reader): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"NCharacterStream\" is not supported"
        )

    override def setNCharacterStream(
        index: Int,
        x: java.io.Reader,
        length: Long
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"NCharacterStream\" is not supported"
        )

    override def setNClob(index: Int, x: java.io.Reader): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"NClob\" is not supported"
        )

    override def setNClob(
        index: Int,
        x: java.io.Reader,
        length: Long
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"NClob\" is not supported"
        )

    override def setNClob(index: Int, x: java.sql.NClob): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"NClob\" is not supported"
        )

    override def setNString(index: Int, x: String): Unit = {
        params += (index -> CharConst(x))
    }

    override def setNull(index: Int, sqlType: Int): Unit = {
        params += (index -> SqlNull(SqlType(sqlType)))
    }

    override def setNull(index: Int, sqlType: Int, typeName: String): Unit = {
        params += (index -> SqlNull(SqlType(sqlType)))
    }


    override def setObject(
        index: Int,
        x: Object,
        sqlType: Int,
        scaleLen: Int
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Function \"setObject\" is not supported"
        )

    override def setObject(index: Int, x: Object, sqlType: Int): Unit =
        throw new SQLFeatureNotSupportedException(
            "Function \"setObject\" is not supported"
        )

    override def setObject(index: Int, x: Object): Unit =
        throw new SQLFeatureNotSupportedException(
            "Function \"setObject\" is not supported"
        )

    override def setRef(index: Int, x: java.sql.Ref): Unit =
        throw new SQLFeatureNotSupportedException(
            "Function \"setRef\" is not supported"
        )

    override def setRowId(index: Int, x: java.sql.RowId): Unit =
        throw new SQLFeatureNotSupportedException(
            "Function \"setRowId\" is not supported"
        )

    override def setShort(index: Int, x: Short): Unit = {
        params += (index -> ShortConst(x))
    }

    override def setSQLXML(index: Int, x: java.sql.SQLXML): Unit =
        throw new SQLFeatureNotSupportedException(
            "Function \"setSQLXML\" is not supported"
        )

    override def setString(index: Int, x: String): Unit = {
        params += (index -> CharConst(x))
    }

    override def setTime(index: Int, x: java.sql.Time): Unit = {
        params += (index -> TimeConst(x))
    }

    override def setTime(
        index: Int,
        x: java.sql.Time,
        cal: java.util.Calendar
    ): Unit = {
        params += (index -> TimeConst(x))
    }

    override def setTimestamp(index: Int, x: java.sql.Timestamp): Unit = {
        params += (index -> TimestampConst(x))
    }

    override def setTimestamp(
        index: Int,
        x: java.sql.Timestamp,
        cal: java.util.Calendar
    ): Unit = {
        params += (index -> TimestampConst(x))
    }

    override def setUnicodeStream(
        index: Int,
        x: java.io.InputStream,
        length: Int
    ): Unit =
        throw new SQLFeatureNotSupportedException(
            "Type \"UnicodeStream\" is not supported"
        )

    override def setURL(index: Int, x: java.net.URL): Unit =
        throw new SQLFeatureNotSupportedException(
            "Function \"setURL\" is not supported"
        )
}
