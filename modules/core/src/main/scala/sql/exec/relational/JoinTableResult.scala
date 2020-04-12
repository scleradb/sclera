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
import com.scleradb.sql.expr.{JoinType, Inner, LeftOuter, RightOuter, FullOuter}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableResult

/** Computes the join two input streams */
abstract class JoinTableResult extends TableResult {
    val joinType: JoinType
    protected def lhsColumns: List[Column]
    protected def rhsColumns: List[Column]

    protected def lhsResultOrder: List[SortExpr]

    private def nullable(cols: List[Column]): List[Column] = cols.map {
        case Column(name, sqlType, fOpt) => Column(name, sqlType.option, fOpt)
    }

    override val columns: List[Column] = joinType match {
        case Inner => lhsColumns:::rhsColumns
        case LeftOuter => lhsColumns:::nullable(rhsColumns)
        case RightOuter => nullable(lhsColumns):::rhsColumns
        case FullOuter => nullable(lhsColumns):::nullable(rhsColumns)
    }

    override val resultOrder: List[SortExpr] = joinType match {
        case (LeftOuter | Inner) => lhsResultOrder
        case (RightOuter | FullOuter) => Nil // NULLs invalidate LHS join order
    }

    override def close(): Unit = { }
}
