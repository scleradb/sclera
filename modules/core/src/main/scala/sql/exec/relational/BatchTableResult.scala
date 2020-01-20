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

import com.scleradb.config.ScleraConfig
import com.scleradb.util.tools.Counter

import com.scleradb.dbms.location.Location

import com.scleradb.exec.{Processor, Planner}

import com.scleradb.objects.Temporary

import com.scleradb.sql.expr._
import com.scleradb.sql.objects.{Table, SchemaTable}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, TableRow}
import com.scleradb.sql.plan.RelEvalPlan

/** Computes an operator result externally in a batch stream.
  * The operator must distribute over the input batches.
  * Each result batch ordered on the given ordering must result in the
  * overall result ordered on the same ordering.
  * @param processor Sclera processor
  * @param op The operator being evaluated
  * @param inputCols Input columns
  * @param inputRows The streaming input on which the operator is evaluated
  * @param resultOrder Sort order of the result
  */
private[scleradb]
class BatchTableResult(
    processor: Processor,
    op: RelOp,
    inputCols: List[Column],
    inputRows: Iterator[Iterator[TableRow]],
    override val resultOrder: List[SortExpr]
) extends TableResult {
    private val tableName: String = Counter.nextSymbol("B")
    private lazy val st: SchemaTable =
        processor.createTable(
            Table(tableName, inputCols, None, Nil, Table.BaseTable),
            Temporary, Some(Location.dataCacheLocationId)
        )

    override lazy val columns: List[Column] = {
        // build a query for the result and evaluate
        // result is empty because base table is empty
        val query: RelExpr =
            RelOpExpr(
                Select(BoolConst(false)), List(
                    RelOpExpr(
                        op, List(TableRefSourceExplicit(processor.schema, st))
                    )
                )
            )

        val plan: RelEvalPlan = processor.planner.planRelEval(query)

        plan.init()
        try plan.result.tableResult.columns finally plan.dispose()
    }

    override def rows: Iterator[TableRow] = new Iterator[TableRow] {
        var curPlanOpt: Option[RelEvalPlan] = None
        var curRowIter: Iterator[TableRow] = Iterator.empty

        // using if/else instead of relying on || computation optimization
        override def hasNext: Boolean = try {
            if( curRowIter.hasNext ) true else {
                curPlanOpt.foreach { plan =>
                    curPlanOpt = None
                    plan.dispose()
                }

                if( inputRows.hasNext ) {
                    processor.delete(st.id)

                    val inputBatch: Iterator[TableRow] = inputRows.next()
                    processor.insert(
                        TableRefTargetExplicit(processor.schema, st),
                        inputCols, inputBatch
                    )

                    val baseQuery: RelExpr = RelOpExpr(
                        op, List(TableRefSourceExplicit(processor.schema, st))
                    )
                    val query: RelExpr = resultOrder match {
                        case Nil => baseQuery
                        case order =>
                            RelOpExpr(Order(order), List(baseQuery))
                    }

                    val plan: RelEvalPlan = processor.planner.planRelEval(query)

                    curPlanOpt = Some(plan)

                    plan.init()
                    curRowIter = plan.result.tableResult.rows

                    // need to recurse as the query result may be empty
                    hasNext
                } else false
            }
        } catch { case (e: Throwable) =>
            curRowIter = Iterator.empty
            curPlanOpt.foreach { plan =>
                curPlanOpt = None
                plan.dispose()
            }

            throw e
        }

        override def next(): TableRow = try {
            if( hasNext ) curRowIter.next() else Iterator.empty.next()
        } catch { case (e: Throwable) =>
            curRowIter = Iterator.empty
            curPlanOpt.foreach { plan =>
                curPlanOpt = None
                plan.dispose()
            }

            throw e
        }
    }

    override def close(): Unit = processor.drop(st.id)
}

/** Companion object containing the constructor */
private[scleradb]
object BatchTableResult {
    def apply(
        processor: Processor,
        op: RelOp,
        inputCols: List[Column],
        inputRows: Iterator[Iterator[TableRow]],
        resultOrder: List[SortExpr]
    ): BatchTableResult = {
        // make columns nullable
        val updInputCols: List[Column] =
            inputCols.map { case Column(name, sqlType, familyOpt) =>
                Column(name, sqlType.option, familyOpt)
            }

        new BatchTableResult(
            processor, op, updInputCols, inputRows, resultOrder
        )
    }
}
