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

import scala.language.postfixOps

import com.scleradb.config.ScleraConfig
import com.scleradb.exec.Processor

import com.scleradb.sql.expr._
import com.scleradb.sql.types.SqlType
import com.scleradb.sql.statements.SqlRelQueryStatement
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow, ConcatTableRow}

/** Computes the equi-join a stream (lhs) and a relational expression (rhs)
  * using a nested loops with lhs in the outer loop and rhs in the inner loop
  * @param processor Sclera processor
  * @param joinType Join type
  * @param lhsInput Left input stream
  * @param lhsCol Join column of the left input
  * @param rhsTableRef Right input expression
  * @param rhsColumns Right input columns
  * @param rhsCol Join column of the right input
  */
class EquiNestedLoopsJoinTableResult(
    processor: Processor,
    override val joinType: JoinType,
    lhsInput: TableResult,
    lhsCol: ColRef,
    rhsInput: RelExpr,
    override val rhsColumns: List[Column],
    rhsCol: ColRef
) extends JoinTableResult {
    assert(joinType == LeftOuter || joinType == Inner)

    val lhsColType: SqlType = lhsInput.column(lhsCol).sqlType.baseType

    override def lhsColumns: List[Column] = lhsInput.columns
    override def lhsResultOrder: List[SortExpr] = lhsInput.resultOrder

    private val batchSize: Int = ScleraConfig.batchSize

    override def rows: Iterator[ScalTableRow] = {
        val rhsNullRow: ScalTableRow = ScalTableRow(
            rhsColumns.map { col => (col.name -> SqlNull(col.sqlType)) }
        )

        lhsInput.typedRows.grouped(batchSize).flatMap { lhsBatch =>
            val lhsVals: List[ScalValueBase] = lhsBatch.toList.flatMap { t =>
                t.getScalExpr(lhsCol) match {
                    case (_: SqlNull) => None // remove NULLs
                    case (v: ScalValueBase) => Some(v)
                }
            } distinct

            val rhsPred:ScalExpr = ScalOpExpr(
                Equals, List(
                    ScalOpExpr(TypeCast(lhsColType), List(rhsCol)),
                    ScalCmpRelExpr(CmpAny, ScalarList(lhsVals))
                )
            )

            val rhsQuery: RelExpr = RelOpExpr(Select(rhsPred), List(rhsInput))
            val rhsStmt: SqlRelQueryStatement = SqlRelQueryStatement(rhsQuery)
            val rhsGroups: Map[Option[Any], List[ScalTableRow]] =
                processor.handleQueryStatement(rhsStmt, { rs =>
                    val ts: List[ScalTableRow] = rs.typedRows.toList
                    ts.groupBy { t => t.getScalExpr(rhsCol).coverValue }
                }
            )

            lhsBatch.flatMap { lhsRow =>
                val rhsMatchedRows: List[ScalTableRow] =
                    lhsRow.getScalExpr(lhsCol) match {
                        case (joinVal: ScalValueBase) =>
                            rhsGroups.get(joinVal.coverValue) getOrElse Nil
                        case (_: SqlNull) => Nil
                    }

                if( rhsMatchedRows.isEmpty ) {
                    if( joinType == LeftOuter )
                        List(ConcatTableRow(lhsRow, rhsNullRow))
                    else Nil
                } else rhsMatchedRows.map { rhsRow =>
                    ConcatTableRow(lhsRow, rhsRow)
                }
            }
        }
    }
}

/** Companion object containing the constructor */
object EquiNestedLoopsJoinTableResult {
    def apply(
        processor: Processor,
        joinType: JoinType,
        lhsInput: TableResult,
        lhsCol: ColRef,
        rhsInput: RelExpr,
        rhsCol: ColRef
    ): EquiNestedLoopsJoinTableResult = {
        // build an empty query for the result and evaluate
        val rhsColumns: List[Column] = rhsInput match {
            case (tRef: TableRef) => tRef.table.columns
            case _ =>
                val rhsColQuery: RelExpr =
                    RelOpExpr(Select(BoolConst(false)), List(rhsInput))
                val rhsColStmt: SqlRelQueryStatement =
                    SqlRelQueryStatement(rhsColQuery)
                processor.handleQueryStatement(
                    rhsColStmt, { rs => rs.columns }
                )
        }

        new EquiNestedLoopsJoinTableResult(
            processor, joinType, lhsInput, lhsCol, rhsInput, rhsColumns, rhsCol
        )
    }
}
