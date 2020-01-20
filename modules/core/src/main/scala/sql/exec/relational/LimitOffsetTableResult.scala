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

import com.scleradb.sql.expr.SortExpr
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, TableRow}

/** Computes LIMIT/OFFSET on the input
  * @param limitOpt Limit (optional)
  * @param offset Offset
  * @param inputResult Streaming input on which the operator is evaluated
  */
private[scleradb]
class LimitOffsetTableResult(
    limitOpt: Option[Int],
    offset: Int,
    inputResult: TableResult
) extends TableResult {
    override val columns: List[Column] = inputResult.columns

    override val resultOrder: List[SortExpr] = inputResult.resultOrder

    override def rows: Iterator[TableRow] = limitOpt match {
        case Some(limit) => inputResult.rows.drop(offset).take(limit)
        case None => inputResult.rows.drop(offset)
    }

    override def close(): Unit = { }
}

/** Companion object containing the constructor */
private[scleradb]
object LimitOffsetTableResult {
    def apply(
        limitOpt: Option[Int],
        offset: Int,
        inputResult: TableResult
    ): LimitOffsetTableResult =
        new LimitOffsetTableResult(limitOpt, offset, inputResult)
}
