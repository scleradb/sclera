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

package com.scleradb.interfaces.jdbc.test

import java.util.Properties
import java.sql.{Connection, Statement, ResultSet, ResultSetMetaData}
import java.sql.{DriverManager, Types, SQLException}

import org.scalatest.CancelAfterFailure
import org.scalatest.funspec.AnyFunSpec

class JDBCTestSuite extends AnyFunSpec with CancelAfterFailure {
    val jdbcUrl: String = "jdbc:scleradb"

    var conn: Connection = null
    var stmt: Statement = null
            
    describe("JDBC driver") {
        it("should return a valid connection") {
            val props: Properties = new Properties()
            props.setProperty("connectTimeout", "10")
            props.setProperty("loginTimeout", "10")

            conn = DriverManager.getConnection(jdbcUrl, props)
        }

        it("should create a valid statement") {
            stmt = conn.createStatement(
                ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
            )
        }

        it("should execute a query") {
            val rs: ResultSet = stmt.executeQuery("select \"b'ar\" as foo;")
            val metaData: ResultSetMetaData = rs.getMetaData()

            assert(metaData.getColumnCount() === 1)
            assert(metaData.getColumnName(1) === "FOO")
            assert(
                (metaData.getColumnType(1) == Types.VARCHAR) ||
                (metaData.getColumnType(1) == Types.CHAR)
            )

            assert(rs.next() === true)
            assert(rs.getString(1) === "b'ar")
            assert(rs.getString("foo") === "b'ar")
            assert(rs.getString(rs.findColumn("foo")) === "b'ar")
            assert(rs.next() === false)

            rs.close()
        }

        it("should close the statement") {
            stmt.close()

            val e: Throwable = intercept[SQLException] {
                stmt.executeQuery("select 1::int as foo;")
            }

            assert(e.getMessage() === "Statement closed")
        }

        it("should close the connection") {
            conn.close()

            val e: Throwable = intercept[SQLException] {
                conn.createStatement(
                    ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY
                )
            }

            assert(e.getMessage() === "Connection closed")
        }
    }
}

// repeat the test -- tests reconnection after connection close
class RepeatJDBCTestSuite extends JDBCTestSuite
