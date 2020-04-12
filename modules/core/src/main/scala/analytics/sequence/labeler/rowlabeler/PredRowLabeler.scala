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

package com.scleradb.analytics.sequence.labeler

import scala.language.postfixOps

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.expr.{ScalExpr, RelExpr}
import com.scleradb.sql.result.ScalTableRow

import com.scleradb.analytics.sequence.labeler.service.PredLabelerService

// labels rows based on predicates
abstract class PredRowLabeler extends RowLabeler {
    val predLabels: List[(ScalExpr, Label)]

    override def rowLabels(row: ScalTableRow): List[Label]
    override def labeledExpr(input: RelExpr): RelExpr

    def clone(newPredLabels: List[(ScalExpr, Label)]): PredRowLabeler

    override def isRedundant(labels: List[Label]): Boolean = false

    override def repr: String = {
        val predLabelStrs: List[String] =
            predLabels.map {
                case (expr, label) => expr.repr + " -> " + label.id
            }

        "Labeled as (" + predLabelStrs.mkString(", ") + ")"
    }
}

object PredRowLabeler {
    def apply(
        labelerIdOpt: Option[String],
        predLabels: List[(ScalExpr, Label)]
    ): PredRowLabeler =
        PredLabelerService(labelerIdOpt).createLabeler(predLabels)

    def apply(predLabels: List[(ScalExpr, Label)]): PredRowLabeler =
        apply(None, predLabels)
}
