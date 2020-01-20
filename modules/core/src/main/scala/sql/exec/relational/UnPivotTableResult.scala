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

import com.scleradb.sql.expr.{ColRef, ScalColValue, CharConst, SortExpr}
import com.scleradb.sql.types.{SqlType, SqlCharVarying}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow}

/** Unpivots the rows based on the given specs
  * @param outValColRef Column containing the unpivoted values
  * @param outKeyColRef Column containing the mapped input column name
  * @param inColRefVals Input column name to value mapping
  */
private[scleradb]
class UnPivotTableResult(
    outValColRef: ColRef,
    outKeyColRef: ColRef,
    inColRefVals: List[(ColRef, CharConst)],
    inputResult: TableResult
) extends TableResult {
    private val inColVals: List[(Column, CharConst)] =
        inColRefVals.map { case (col, v) => inputResult.column(col) -> v }
    private val inCols: List[Column] = inColVals.map { case (col, _) => col }

    private val outValType: SqlType = inCols.headOption match {
        case Some(col) => col.sqlType
        case None => SqlCharVarying(None)
    }

    private val outValCol: Column = Column(outValColRef.name, outValType)
    private val outKeyCol: Column =
        Column(outKeyColRef.name, SqlCharVarying(None))
    private val remCols: List[Column] = inputResult.columns.diff(inCols)

    override val columns: List[Column] = outValCol :: outKeyCol :: remCols
    
    override val resultOrder: List[SortExpr] = {
        val inColRefs: List[ColRef] =
            inColRefVals.map { case (col, _) => col }
        inputResult.resultOrder.takeWhile { case SortExpr(expr, _, _) =>
            expr.colRefs.forall { col => !inColRefs.contains(col) }
        }
    }

    override def rows: Iterator[ScalTableRow] =
        inputResult.rows.flatMap { row =>
            val remColVals: List[(String, ScalColValue)] =
                remCols.map { col => col.name -> row.getScalExpr(col) }
            inColVals.map { case (inCol, inVal) =>
                ScalTableRow(
                    outValColRef.name ->
                        row.getScalExpr(inCol.name, outValType) ::
                    outKeyColRef.name -> inVal ::
                    remColVals
                )
            }
        }

    override def close(): Unit = { }
}

/** Companion object containing the constructor */
private[scleradb]
object UnPivotTableResult {
    def apply(
        outValCol: ColRef,
        outKeyCol: ColRef,
        inColVals: List[(ColRef, CharConst)],
        inputResult: TableResult
    ): UnPivotTableResult =
        new UnPivotTableResult(outValCol, outKeyCol, inColVals, inputResult)
}
