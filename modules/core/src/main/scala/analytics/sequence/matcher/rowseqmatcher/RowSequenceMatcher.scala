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
import com.scleradb.util.automata.nfa.AnchoredNfa
import com.scleradb.util.regexparser.{RegexParser, RegexSuccess, RegexFailure}

import com.scleradb.sql.expr.{ColRef, SortExpr}
import com.scleradb.sql.result.{TableResult, ScalTableRow}
import com.scleradb.sql.exec.ScalExprEvaluator

import com.scleradb.analytics.sequence.matcher.aggregate._
import com.scleradb.analytics.sequence.matcher.service.SequenceMatcherService

private[scleradb]
abstract class RowSequenceMatcher extends java.io.Serializable {
    val aggregateRowsSpec: SeqAggregateRowsSpec
    val partitionCols: List[ColRef]

    def isArgMatch: Boolean = aggregateRowsSpec.isArgAggregate

    def tableColRefs(inpTableCols: List[ColRef]): List[ColRef]

    def resultOrder(inpOrder: List[SortExpr]): List[SortExpr]

    def matchResult(
        evaluator: ScalExprEvaluator,
        rowLabels: ScalTableRow => List[Label],
        input: TableResult
    ): TableResult

    def clone(
        newAggregateRowsSpec: SeqAggregateRowsSpec,
        newPartitionCols: List[ColRef]
    ): RowSequenceMatcher

    def repr: List[String] = {
        val partnStrs: List[String] = partitionCols match {
            case Nil => Nil
            case partnCols =>
                val partnStrs: List[String] = partnCols.map { c => c.repr }
                List("[Partition by: " + partnStrs.mkString(", ") + "]")
        }

        val aggRowSpecStrs: List[String] = aggregateRowsSpec match {
            case SeqArgOptsSpec(aggregateColSpecs) =>
                aggregateColSpecs.map { a => "ARG " + a.repr }

            case SeqAggregateColSetSpec(aggregateColSpecs, retainedCols) =>
                val aggrColReprs: List[String] =
                    aggregateColSpecs.map { a => a.repr }

                if( retainedCols.isEmpty ) aggrColReprs else {
                    val retainedColReprs: List[String] =
                        retainedCols.map { col => col.repr + " -> " + col.repr }

                    retainedColReprs:::aggrColReprs
                }
        }

        if( aggRowSpecStrs.isEmpty ) partnStrs else {
            ("[" + aggRowSpecStrs.mkString(", ") + "]")::partnStrs
        }
    }
}

private[scleradb]
object RowSequenceMatcher {
    def apply(
        nfa: AnchoredNfa,
        aggregateSpec: SeqAggregateRowsSpec,
        partitionCols: List[ColRef]
    ): RowSequenceMatcher =
        SequenceMatcherService(None).createMatcher(
            nfa, aggregateSpec, partitionCols
        )

    def apply(
        regexStr: String,
        aggregateSpec: SeqAggregateRowsSpec,
        partitionCols: List[ColRef]
    ): RowSequenceMatcher = {
        val nfa: AnchoredNfa =
            RegexParser.parse(regexStr.toUpperCase) match {
                case RegexSuccess(nfa) => nfa
                case RegexFailure(msg) =>
                    throw new IllegalArgumentException(msg)
            }

        apply(nfa, aggregateSpec, partitionCols)
    }
}
