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

import com.scleradb.sql.expr._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ScalTableRow, ConcatTableRow}
import com.scleradb.sql.result.{TableRowGroup, TableRowGroupIterator}

/** Computes the equi-join two input streams
  * @param evaluator Scalar expression evaluator
  * @param joinType Join type
  * @param lhsInput Left input stream
  * @param lhsCol Join column of the left input stream
  * @param rhsInput Right input stream
  * @param rhsCol Join column of the right input stream
  */
class EquiMergeJoinTableResult(
    evaluator: ScalExprEvaluator,
    override val joinType: JoinType,
    lhsInput: TableResult,
    lhsCol: ColRef,
    rhsInput: TableResult,
    rhsCol: ColRef
) extends JoinTableResult {
    override def lhsColumns: List[Column] = lhsInput.columns
    override def rhsColumns: List[Column] = rhsInput.columns

    override def lhsResultOrder: List[SortExpr] = lhsInput.resultOrder

    override def rows: Iterator[ScalTableRow] = {
        val lhsNullRow: ScalTableRow = ScalTableRow(
            lhsInput.columns.map { col => (col.name -> SqlNull(col.sqlType)) }
        )
        val rhsNullRow: ScalTableRow = ScalTableRow(
            rhsInput.columns.map { col => (col.name -> SqlNull(col.sqlType)) }
        )

        val alignedGroupsIter: AlignedGroupsIterator =
            new AlignedGroupsIterator(
                evaluator, joinType, lhsInput, lhsCol, rhsInput, rhsCol
            )

        alignedGroupsIter.flatMap {
            case (Some(lhsGroup), Some(rhsGroup)) =>
                lhsGroup.rows.flatMap { lhsRow =>
                    rhsGroup.rows.map { rhsRow =>
                        ConcatTableRow(lhsRow, rhsRow)
                    }
                }
            case (Some(lhsGroup), None) =>
                lhsGroup.rows.map { row =>
                    ConcatTableRow(row, rhsNullRow)
                }
            case (None, Some(rhsGroup)) =>
                rhsGroup.rows.map { row =>
                    ConcatTableRow(lhsNullRow, row)
                }
            case (None, None) => Iterator.empty
        }
    }
}

/** Companion object containing the constructor */
object EquiMergeJoinTableResult {
    def apply(
        evaluator: ScalExprEvaluator,
        joinType: JoinType,
        lhsInp: TableResult,
        lhsCol: ColRef,
        rhsInp: TableResult,
        rhsCol: ColRef
    ): EquiMergeJoinTableResult =
        new EquiMergeJoinTableResult(
            evaluator, joinType, lhsInp, lhsCol, rhsInp, rhsCol
        )
}

/** Groups the two input streams according to the respective join columns,
  * and then aligns the two sequence of groups based on the join column values
  * and the join type.
  * @param evaluator Scalar expression evaluator
  * @param joinType Join type
  * @param lhsInput Left input stream
  * @param lhsCol Join column of the left input stream
  * @param rhsInput Right input stream
  * @param rhsCol Join column of the right input stream
  */
class AlignedGroupsIterator(
    evaluator: ScalExprEvaluator,
    joinType: JoinType,
    lhsInput: TableResult,
    lhsCol: ColRef,
    rhsInput: TableResult,
    rhsCol: ColRef
) extends Iterator[(Option[TableRowGroup], Option[TableRowGroup])] {
    private val lhsGroups: Iterator[TableRowGroup] =
        TableRowGroupIterator(evaluator, lhsInput.typedRows, List(lhsCol))
    private val rhsGroups: Iterator[TableRowGroup] =
        TableRowGroupIterator(evaluator, rhsInput.typedRows, List(rhsCol))

    private def headOption(it: Iterator[TableRowGroup]): Option[TableRowGroup] =
        if( it.hasNext ) Some(it.next) else None

    private var lhsLookaheadOpt: Option[TableRowGroup] = headOption(lhsGroups)
    private var rhsLookaheadOpt: Option[TableRowGroup] = headOption(rhsGroups)

    private def nextPairOpt:
    Option[(Option[TableRowGroup], Option[TableRowGroup])] = {
        ((lhsLookaheadOpt, rhsLookaheadOpt), joinType) match {
            case (pair@(Some(lhs), Some(rhs)), _)
            if isMatch(lhs, rhs) =>
                lhsLookaheadOpt = headOption(lhsGroups)
                rhsLookaheadOpt = headOption(rhsGroups)
                Some(pair)

            case ((lhsOpt@Some(lhs), Some(rhs)), LeftOuter | FullOuter)
            if precedes(lhs.groupColVal(lhsCol), rhs.groupColVal(rhsCol)) =>
                lhsLookaheadOpt = headOption(lhsGroups)
                Some((lhsOpt, None))
            case ((Some(lhs), rhsOpt@Some(rhs)), RightOuter | FullOuter)
            if precedes(rhs.groupColVal(rhsCol), lhs.groupColVal(lhsCol)) =>
                rhsLookaheadOpt = headOption(rhsGroups)
                Some((None, rhsOpt))

            case ((Some(lhs), Some(rhs)), RightOuter | Inner)
            if precedes(lhs.groupColVal(lhsCol), rhs.groupColVal(rhsCol)) =>
                lhsLookaheadOpt = headOption(lhsGroups)
                Some((None, None))
            case ((Some(lhs), Some(rhs)), LeftOuter | Inner)
            if precedes(rhs.groupColVal(rhsCol), lhs.groupColVal(lhsCol)) =>
                rhsLookaheadOpt = headOption(rhsGroups)
                Some((None, None))

            case (pair@(Some(_), None), LeftOuter | FullOuter) =>
                lhsLookaheadOpt = headOption(lhsGroups)
                Some(pair)
            case (pair@(None, Some(_)), RightOuter | FullOuter) =>
                rhsLookaheadOpt = headOption(rhsGroups)
                Some(pair)

            case ((Some(_), None), RightOuter | Inner) =>
                None
            case ((None, Some(_)), LeftOuter | Inner) =>
                None
            case ((None, None), _) =>
                None
        }
    }

    private val (sortDir, nullsOrder) = lhsInput.resultOrder match {
        case SortExpr(col: ColRef, lhsSortDir, lhsNullsOrder)::_
        if (col == lhsCol) => 
            (lhsSortDir, lhsNullsOrder)
        case _ =>
            throw new RuntimeException(
                "Expected sort on column \"" + lhsCol.repr + "\""
            )
    }

    private def precedes(
        aValOpt: ScalColValue,
        bValOpt: ScalColValue
    ): Boolean = (aValOpt, bValOpt, sortDir, nullsOrder) match {
        case ((aVal: ScalValueBase), (bVal: ScalValueBase), SortAsc, _) =>
            (aVal compare bVal) < 0
        case ((aVal: ScalValueBase), (bVal: ScalValueBase), SortDesc, _) =>
            (aVal compare bVal) > 0
        case ((_: SqlNull), (_: ScalValueBase), SortAsc, NullsFirst) => true
        case ((_: SqlNull), (_: ScalValueBase), SortAsc, NullsLast) => false
        case ((_: SqlNull), (_: ScalValueBase), SortDesc, NullsFirst) => false
        case ((_: SqlNull), (_: ScalValueBase), SortDesc, NullsLast) => true
        case ((_: ScalValueBase), (_: SqlNull), SortAsc, NullsFirst) => false
        case ((_: ScalValueBase), (_: SqlNull), SortAsc, NullsLast) => true
        case ((_: ScalValueBase), (_: SqlNull), SortDesc, NullsFirst) => true
        case ((_: ScalValueBase), (_: SqlNull), SortDesc, NullsLast) => false
        case ((_: SqlNull), (_: SqlNull), _, _) => true
    }

    private def isMatch(lhs: TableRowGroup, rhs: TableRowGroup): Boolean =
        (lhs.groupColVal(lhsCol), rhs.groupColVal(rhsCol)) match {
            case (lhsVal: ScalValueBase, rhsVal: ScalValueBase) =>
                (lhsVal compare rhsVal) == 0
            case _ => false
        }

    // the iterator streams out successive values of nextPairOpt

    private var lookaheadOpt:
    Option[(Option[TableRowGroup], Option[TableRowGroup])] = nextPairOpt

    override def hasNext: Boolean = !lookaheadOpt.isEmpty

    override def next(): (Option[TableRowGroup], Option[TableRowGroup]) = {
        val pair: (Option[TableRowGroup], Option[TableRowGroup]) =
            lookaheadOpt getOrElse Iterator.empty.next()
        lookaheadOpt = nextPairOpt

        pair
    }
}
