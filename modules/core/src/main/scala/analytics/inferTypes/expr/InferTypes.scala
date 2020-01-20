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

package com.scleradb.analytics.infertypes.expr

import com.scleradb.sql.expr.{RelExpr, ProjectBase, ExtendedRelOp}
import com.scleradb.sql.expr.{ColRef, AnnotColRef, SortExpr}
import com.scleradb.sql.expr.{TargetExpr, AliasedExpr, ScalarTarget, RenameCol}

private[scleradb]
case class InferTypes(
    cols: List[ColRef],
    nulls: List[String],
    lookAheadOpt: Option[Int]
) extends ExtendedRelOp {
    override val arity: Int = 1

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head.resultOrder takeWhile { se =>
            val seCols: Set[ColRef] = se.expr.colRefs
            !cols.exists(col => seCols contains col)
        }

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs
    override def tableNames(inputs: List[RelExpr]): List[String] = Nil
    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns
}

object InferTypes {
    def apply(
        targets: List[TargetExpr],
        nulls: List[String],
        lookAheadOpt: Option[Int],
        input: RelExpr
    ): InferTypes = {
        val expanded: List[ScalarTarget] =
            ProjectBase.expandedTargetExprs(targets, input)

        val cols: List[ColRef] = expanded.flatMap {
            case AliasedExpr(AnnotColRef(_, name), _) => Some(ColRef(name))
            case RenameCol(col, _) => Some(col)
            case _ => None
        }

        InferTypes(cols, nulls, lookAheadOpt)
    }
}
