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

import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableRow

class SqlTestSuite extends AnyFunSpec with CancelAfterFailure {
    var processor: Processor = null

    describe("Sql Query Processing") {
        it("should setup") {
            processor = Processor()
            processor.init()
        }

        it("should execute the SQL queries") {
            val query: String =
                "values (1), (2) as X(A)"

            processor.handleStatement(query, { ts =>
                val cols: List[Column] = ts.columns
                assert(cols.size === 1)

                assert(cols(0).name === "A")

                val rows: List[TableRow] = ts.rows.toList
                assert(rows.size === 2)

                assert(rows(0).getIntOpt("A") === Some(1))
                assert(rows(1).getIntOpt("A") === Some(2))
            })
        }

        it("should teardown") {
            processor.close()
        }
    }
}
