/**
* Sclera - Regular Expression Parser
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

package com.scleradb.util.regexparser.test

import org.scalatest.CancelAfterFailure
import org.scalatest.funspec.AnyFunSpec

import com.scleradb.util.regexparser._

class RegexParseSuite extends AnyFunSpec with CancelAfterFailure {
    describe("regular expressions") {
        it("should parse") {
            List("A*", "A+", "A?", "A+*", "A+?", "X*Y",
                 "A|B", "(A.B)", "A.(B|C)", "A|(B.C)",
                 "A B", "(A)(B)", "^(A|B)$", "hello world").foreach {
                r => RegexParser.parse(r) match {
                    case RegexSuccess(nfa) => ()
                    case RegexFailure(msg) => assert(false, msg)
                }
            }
        }
    }
}
