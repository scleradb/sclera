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

package com.scleradb.analytics.sequence.matcher.aggregate

import scala.language.postfixOps
import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.result.TableResult
import com.scleradb.sql.expr.{AliasedExpr, ColRef}

sealed abstract class SeqAggregateRowsSpec extends java.io.Serializable {
    def isArgAggregate: Boolean

    val aggregateColSpecs: List[SeqAggregateColSpec]
    val specLabels: Set[Label] =
        aggregateColSpecs.flatMap { s => s.labels } toSet

    def resultColRefs(inpTableCols: List[ColRef]): List[ColRef]

    def resultPartnCols(inpPartnCols: List[ColRef]): List[ColRef]

    def aggregate(input: TableResult): SeqAggregateRows
}

case class SeqArgOptsSpec(
    override val aggregateColSpecs: List[SeqAggregateColSpec]
) extends SeqAggregateRowsSpec {
    override def isArgAggregate: Boolean = true

    override def resultColRefs(inpTableCols: List[ColRef]): List[ColRef] =
        inpTableCols

    override def resultPartnCols(inpPartnCols: List[ColRef]): List[ColRef] =
        inpPartnCols

    override def aggregate(input: TableResult): SeqArgOpts = {
        val aggregates: List[SeqArgAggregate] =
            aggregateColSpecs.map { a =>
                SeqArgAggregate(input.columns, a.aggregate(input))
            }

        SeqArgOpts(aggregates)
    }
}

case class SeqAggregateColSetSpec(
    override val aggregateColSpecs: List[SeqAggregateColSpec],
    retainedColRefs: List[ColRef]
) extends SeqAggregateRowsSpec {
    override def isArgAggregate: Boolean = false

    override def resultColRefs(inpTableCols: List[ColRef]): List[ColRef] = {
        val aggrColRefs: List[ColRef] =
            aggregateColSpecs.map { a => a.resultColRef }

        retainedColRefs:::aggrColRefs
    }

    override def resultPartnCols(inpPartnCols: List[ColRef]): List[ColRef] =
        inpPartnCols intersect retainedColRefs

    override def aggregate(input: TableResult): SeqAggregateColSet =
        SeqAggregateColSet(
            aggregateColSpecs.map { a => a.aggregate(input) },
            retainedColRefs.map { col => input.column(col) }
        )
}
