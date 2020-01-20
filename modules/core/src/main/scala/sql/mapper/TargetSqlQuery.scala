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

package com.scleradb.sql.mapper.target

import com.scleradb.sql.expr._

/** Internal representation of a SQL query - to be translated to SQL string */
sealed abstract class TargetSqlQuery

/** SQL SELECT statement */
case class TargetSqlSelect(
    distinct: Option[List[ScalExpr]] = None,
    select: List[TargetExpr] = List(StarTargetExpr(None)),
    from: TargetSqlFrom,
    where: Option[ScalExpr] = None,
    group: List[ScalExpr] = Nil,
    having: Option[ScalExpr] = None,
    order: List[SortExpr] = Nil,
    limit: Option[Int] = None,
    offset: Option[Int] = None,
    isAggregate: Boolean = false
) extends TargetSqlQuery

/** SQL compound statement */
case class TargetSqlCompound(
    compoundType: CompoundType,
    lhs: TargetSqlQuery,
    rhs: TargetSqlQuery
) extends TargetSqlQuery

/** SQL FROM clause */
sealed abstract class TargetSqlFrom

/** SQL table reference */
case class TargetSqlTableRef(
    name: String,
    cols: List[ColRef]
) extends TargetSqlFrom

/** SQL table reference */
case class TargetSqlValues(
    name: String,
    cols: List[ColRef],
    rows: List[Row]
) extends TargetSqlFrom {
    require(!rows.isEmpty, "Value list \"" + name + "\" has no rows")
}

/** SQL JOIN */
case class TargetSqlJoin(
    joinType: JoinType,
    joinPred: JoinPred,
    lhsInput: TargetSqlFrom,
    rhsInput: TargetSqlFrom
) extends TargetSqlFrom

/** SQL nested subselect */
case class TargetSqlNested(
    name: String,
    sqlQuery: TargetSqlQuery
) extends TargetSqlFrom
