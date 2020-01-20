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

package com.scleradb.sql.exec

import com.scleradb.sql.expr.{SortExpr, ColRef, ScalColValue, SqlNull, Row}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}

/** Result of a VALUES expression */
private[scleradb]
class ValuesTableResult(
    aliasCols: List[ColRef],
    values: List[Row]
) extends TableResult {
    override val columns: List[Column] = values.headOption match {
        case Some(Row(hs)) => aliasCols.zip(hs).map {
            case (ColRef(name), s) => Column(name, s.sqlBaseType)
        }

        case None => aliasCols.map {
            case ColRef(name) => Column(name, SqlNull().sqlBaseType)
        }
    }
        
    override def rows: Iterator[ScalTableRow] =
        values.iterator.map { case Row(vs) =>
            val valueMap: Map[String, ScalColValue] = Map() ++
                aliasCols.zip(vs).map {
                    case (ColRef(name), s) => (name -> s)
                }

            ScalTableRow(valueMap)
        }

    override val resultOrder: List[SortExpr] = Nil

    override def close(): Unit = { }
}

/** Companion object containing the constructor */
private[scleradb]
object ValuesTableResult {
    def apply(
        aliasCols: List[ColRef],
        values: List[Row]
    ): ValuesTableResult = new ValuesTableResult(aliasCols, values)
}
