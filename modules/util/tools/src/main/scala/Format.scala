/**
* Sclera - Tools
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

package com.scleradb.util.tools

import scala.language.postfixOps

object Format {
    def formatResultSet(
        cnames: List[String],
        rows: List[List[String]]
    ): List[String] = {
        val cwidth: List[Int] = rows.foldLeft (cnames.map { s => s.size }) {
            case (prevSizes, row) => prevSizes.zip(row).map {
                case (prevSize, s) => prevSize max s.size
            }
        }

        val separatorStr: String = cwidth.map { w => "-"*(w+2) } mkString("+")

        def rowStr(vals: List[String]): String = vals.zip(cwidth).map {
            case (s, w) => " " + s.take(w).padTo(w, " ").mkString + " "
        } mkString("|")

        val rowNumStr: String = "(" +
            rows.size + " " + (if( rows.size == 1 ) "row" else "rows") +
        ")"

        List(
            List(separatorStr),
            List(rowStr(cnames)),
            List(separatorStr),
            rows.map { r => rowStr(r) },
            List(separatorStr),
            List(rowNumStr)
        ) flatten
    }
}
