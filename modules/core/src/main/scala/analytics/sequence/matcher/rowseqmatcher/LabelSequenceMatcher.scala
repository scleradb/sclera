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

package com.scleradb.analytics.sequence.matcher

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.exec.ScalExprEvaluator
import com.scleradb.sql.expr.{ColRef, SortExpr}
import com.scleradb.sql.result.{TableResult, ScalTableRow}

import com.scleradb.analytics.sequence.matcher.aggregate._

private[scleradb]
class LabelSequenceMatcher(
    override val aggregateRowsSpec: SeqAggregateRowsSpec,
    override val partitionCols: List[ColRef],
    val isPartnColsInResult: Boolean,
    isEmptyResultAllowed: Boolean
) extends RowSequenceMatcher {
    override def tableColRefs(inpTableCols: List[ColRef]): List[ColRef] = {
        val aggrCols: List[ColRef] =
            aggregateRowsSpec.resultColRefs(inpTableCols)

        if( isPartnColsInResult ) partitionCols:::aggrCols else aggrCols
    }

    override def matchResult(
        evaluator: ScalExprEvaluator,
        rowLabels: ScalTableRow => List[Label],
        input: TableResult
    ): TableResult =
        new LabelSequenceMatchResult(
            evaluator, isEmptyResultAllowed,
            partitionCols, isPartnColsInResult,
            aggregateRowsSpec, rowLabels, input,
            resultOrder(input.resultOrder)
        )
    
    override def resultOrder(inpOrder: List[SortExpr]): List[SortExpr] = {
        val resultPartnCols: List[ColRef] =
            if( isPartnColsInResult ) partitionCols
            else aggregateRowsSpec.resultPartnCols(partitionCols)

        inpOrder.takeWhile { case SortExpr(expr, _, _) =>
            resultPartnCols contains expr
        }
    }

    override def clone(
        newAggregateRowsSpec: SeqAggregateRowsSpec,
        newPartitionCols: List[ColRef]
    ): LabelSequenceMatcher =
        new LabelSequenceMatcher(
            newAggregateRowsSpec, newPartitionCols,
            isPartnColsInResult, isEmptyResultAllowed
        )

    override def toString: String = 
        "LabelSequenceMatcher[" +
        aggregateRowsSpec + ": " +
        "Partn on " + partitionCols.map(col => col.repr).mkString(", ") + " " +
        (if( isPartnColsInResult ) "" else "(hidden)") + "]"
}

private[scleradb]
object LabelSequenceMatcher {
    def apply(
        aggregateRowsSpec: SeqAggregateRowsSpec,
        partitionCols: List[ColRef],
        isPartnColsInResult: Boolean,
        isEmptyResultAllowed: Boolean
    ): LabelSequenceMatcher =
        new LabelSequenceMatcher(
            aggregateRowsSpec, partitionCols,
            isPartnColsInResult, isEmptyResultAllowed
        )
}
