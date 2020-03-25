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

package com.scleradb.test

import org.scalatest.CancelAfterFailure
import org.scalatest.funspec.AnyFunSpec

import com.scleradb.exec.Processor

class SqlParseSuite extends AnyFunSpec with CancelAfterFailure {
    var processor: Processor = null

    describe("SQL Parsing") {
        val validStmts: List[String] = List(
            "create table t(x int primary key)",
            "create table t(x int primary key references t, y int)",
            "create table t(x int primary key, y int references t(x))",
            "create table t(x int primary key, y int, primary key(x))",
            "create table t(x int primary key, y int references t, " +
                            "foreign key(y) references t)",
            "create table t(x int primary key, " +
                           "y int references t references u)",
            "create table t(x int, y int primary key references u)",
            "create table t(x int, y int, primary key(x), " +
                            "foreign key(y) references t(x))",
            "create mahout classifier foo(a) using bar"
        )

        val invalidStmts: List[String] = List(
            "create table t(x int primary key, y int primary key)",
            "create table t(x int primary key, y int, primary key(y))",
            "create table t(x int, y int, primary key(x), primary key(y))"
        )

        it("should setup") {
            processor = Processor(checkSchema = true)
            try processor.init() catch { case (_: java.sql.SQLWarning) =>
                processor.schema.createSchema()
            }
        }

        // needs embedded schema, cache dbs to work
        it("should parse valid CREATE TABLE statements") {
            validStmts.foreach { stmt =>
                processor.parser.parseSqlStatements(stmt)
            }
        }

        // needs embedded schema, cache dbs to work
        it("should parse consolidated valid CREATE TABLE statements") {
            processor.parser.parseSqlStatements(validStmts.mkString("; "))
            processor.parser.parseSqlStatements(validStmts.mkString("; ") + ";")
        }

        it("should not parse invalid CREATE TABLE statements") {
            invalidStmts.foreach { stmt =>
                intercept[IllegalArgumentException] {
                    processor.parser.parseSqlStatements(stmt)
                }
            }
        }

        it("should not parse consolidated invalid CREATE TABLE statements") {
            intercept[IllegalArgumentException] {
                processor.parser.parseSqlStatements(invalidStmts.mkString("; "))
            }
        }

        it("should teardown") {
            processor.close()
        }
    }
}
