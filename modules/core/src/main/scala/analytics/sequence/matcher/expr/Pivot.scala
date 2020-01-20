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

import com.scleradb.util.automata.datatypes.Label
import com.scleradb.sql.expr.{ScalExpr, ColRef}

import com.scleradb.analytics.sequence.matcher.aggregate._
import com.scleradb.analytics.sequence.labeler.ColumnRowLabeler
import com.scleradb.analytics.sequence.matcher.LabelSequenceMatcher

private[scleradb]
object Pivot {
    def apply(
        funcName: String,
        params: List[ScalExpr],
        pivotColRef: ColRef,
        pivotTargets: List[(List[Label], Option[ColRef])],
        partnCols: List[ColRef]
    ): LabeledMatch = {
        val labeler: ColumnRowLabeler = ColumnRowLabeler(pivotColRef)

        val specs: List[SeqFunctionSpec] = pivotTargets.map {
            case (labels, aliasOpt) =>
                val alias: ColRef = aliasOpt getOrElse {
                    ColRef(labels.map(label => label.id).mkString("_"))
                }

                SeqFunctionSpec(alias, funcName, params, labels)
        }

        val matcher: LabelSequenceMatcher =
            LabelSequenceMatcher(
                SeqAggregateColSetSpec(specs, Nil),
                partnCols, true, false
            )

        LabeledMatch(labeler, matcher)
    }
}
