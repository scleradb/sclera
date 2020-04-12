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

package com.scleradb.sql.mapper

import com.scleradb.util.tools.Counter

import com.scleradb.sql.expr._
import com.scleradb.sql.statements._

import com.scleradb.sql.mapper.target._

// translate normalized relExpr to SQL intermediate structures
object SqlTranslator {
    def translateQuery(
        query: SqlRelQueryStatement
    ): TargetSqlQuery = query match {
        case SqlRelQueryStatement(relExpr) => translateNormalizedQuery(relExpr)
    }

    private def translateNormalizedQuery(
        relExpr: RelExpr
    ): TargetSqlQuery = relExpr match {
        case (tRef: TableRefSource) =>
            TargetSqlSelect(from = TargetSqlTableRef(tRef.name, Nil))

        case values@Values(_, rows) =>
            TargetSqlSelect(
                from = TargetSqlValues(values.name, values.tableColRefs, rows)
            )

        case expr@RelOpExpr(TableAlias(name, _, _), List(Values(_, rows)), _) =>
            TargetSqlSelect(
                from = TargetSqlValues(name, expr.tableColRefs, rows)
            )

        case RelOpExpr(Project(targetExprs), List(input), _) =>
            TargetSqlSelect(select = targetExprs,
                            from = targetNormalizedFrom(input))

        case RelOpExpr(Aggregate(targetExprs, groupExprs, predOpt),
                       List(input), _) =>
            TargetSqlSelect(select = targetExprs,
                            from = targetNormalizedFrom(input),
                            group = groupExprs,
                            having = predOpt,
                            isAggregate = true)

        case RelOpExpr(Order(sortExprs), List(input), _) =>
            TargetSqlSelect(from = targetNormalizedFrom(input),
                            order = sortExprs)

        case RelOpExpr(LimitOffset(lim, off, sortExprs), List(input), _) =>
            TargetSqlSelect(from = targetNormalizedFrom(input),
                            order = sortExprs,
                            limit = lim,
                            offset = if( off == 0 ) None else Some(off))

        case RelOpExpr(DistinctOn(exprs, sortExprs), List(input), _) =>
            TargetSqlSelect(distinct = Some(exprs),
                            from = targetNormalizedFrom(input),
                            order = sortExprs)

        case RelOpExpr(Distinct, List(input), _) =>
            TargetSqlSelect(distinct = Some(Nil),
                            from = targetNormalizedFrom(input))

        case RelOpExpr(Select(pred), List(input), _) =>
            TargetSqlSelect(where = Some(pred),
                            from = targetNormalizedFrom(input))

        case RelOpExpr(Join(joinType, joinPred), List(lhs, rhs), _) =>
            TargetSqlSelect(from = TargetSqlJoin(joinType, joinPred,
                                                 targetNormalizedFrom(lhs),
                                                 targetNormalizedFrom(rhs)))

        case RelOpExpr(Compound(compoundType), List(lhs, rhs), _) =>
            TargetSqlCompound(compoundType, translateNormalizedQuery(lhs),
                              translateNormalizedQuery(rhs))

        case RelOpExpr(EvaluateOp, List(input), _) =>
            translateNormalizedQuery(input)

        case _ =>
            throw new RuntimeException("Cannot translate: " + relExpr)
    }

    private def targetNormalizedFrom(
        relExpr: RelExpr
    ): TargetSqlFrom = relExpr match {
        case (tRef: TableRefSource) =>
            TargetSqlTableRef(tRef.name, Nil)

        case values@Values(_, rows) =>
            TargetSqlValues(values.name, values.tableColRefs, rows)

        case expr@RelOpExpr(TableAlias(name, _, _), List(Values(_, rows)), _) =>
            TargetSqlValues(name, expr.tableColRefs, rows)

        case RelOpExpr(Join(joinType, joinPred), List(lhs, rhs), _) =>
            TargetSqlJoin(
                joinType, joinPred,
                targetNormalizedFrom(lhs), targetNormalizedFrom(rhs)
            )

        case _ =>
            TargetSqlNested(
                Counter.nextSymbol("N"), translateNormalizedQuery(relExpr)
            )
    }
}
