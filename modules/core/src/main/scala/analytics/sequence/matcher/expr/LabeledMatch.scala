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

package com.scleradb.analytics.sequence.matcher.expr

import com.scleradb.dbms.location.LocationId

import com.scleradb.sql.expr.{RelExpr, ExtendedRelOp}
import com.scleradb.sql.expr.{ColRef, AnnotColRef, SortExpr}

import com.scleradb.analytics.sequence.labeler.RowLabeler
import com.scleradb.analytics.sequence.matcher.RowSequenceMatcher

private[scleradb]
case class LabeledMatch(
    labeler: RowLabeler,
    matcher: RowSequenceMatcher
) extends ExtendedRelOp {
    override val arity: Int = 1

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        matcher.resultOrder(inputs.head.resultOrder)

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        matcher.tableColRefs(inputs.head.tableColRefs)

    override def tableNames(inputs: List[RelExpr]): List[String] = Nil

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        tableColRefs(inputs).map { col => AnnotColRef(None, col.name) }
}
