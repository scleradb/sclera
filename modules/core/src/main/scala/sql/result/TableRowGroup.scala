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

package com.scleradb.sql.result

import com.scleradb.sql.expr.{ScalExpr, ScalColValue}

/** Contains the rows associated with a group of column values
  * @param groupColValMap Mapping of grouping expressions to their values
  * @param rows           Iterator on the rows associated with the group
  */
class TableRowGroup(
    val groupColValMap: Map[ScalExpr, ScalColValue],
    val rows: Iterator[ScalTableRow]
) {
    def groupColVal(expr: ScalExpr): ScalColValue =
        groupColValMap.get(expr) getOrElse {
            throw new RuntimeException(
                "Expression \"" + expr.repr + "\" is not a group expression" +
                " [Group Map = " + groupColValMap + "]"
            )
        }
}
