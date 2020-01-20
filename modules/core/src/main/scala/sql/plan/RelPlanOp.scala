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

package com.scleradb.sql.plan

import com.scleradb.config.ScleraConfig
import com.scleradb.dbms.location.{Location, LocationId}
import com.scleradb.objects.Temporary
import com.scleradb.exec.Processor
import com.scleradb.plan.Plan

import com.scleradb.sql.expr._
import com.scleradb.sql.objects.{TableId, SchemaTable}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.exec.ScalExprEvaluator

import com.scleradb.sql.result.{TableResult, TableRow, ScalTableRow}
import com.scleradb.sql.result.TableRowGroupIterator

import com.scleradb.sql.exec.DistinctTableResult
import com.scleradb.sql.exec.LimitOffsetTableResult
import com.scleradb.sql.exec.ProjectTableResult
import com.scleradb.sql.exec.SelectTableResult
import com.scleradb.sql.exec.EquiMergeJoinTableResult
import com.scleradb.sql.exec.EquiNestedLoopsJoinTableResult
import com.scleradb.sql.exec.BatchTableResult
import com.scleradb.sql.exec.AlignTableResult
import com.scleradb.sql.exec.DisjointIntervalTableResult
import com.scleradb.sql.exec.UnPivotTableResult

private[scleradb]
abstract class RelPlanOp {
    def init(): Unit = { /* empty */ }
    def dispose(): Unit = { /* empty */ }
}

private[scleradb]
abstract class RelExprOp extends RelPlanOp {
    def expr(inputResults: List[RelExprPlanResult]): RelExprPlanResult
}

private[scleradb]
case class RegularRelExprOp(
    preparedRelOp: RegularRelOp,
    opDepsPlans: List[RelExprPlan]
) extends RelExprOp {
    override def init(): Unit = Plan.cleanInit(opDepsPlans.reverse)

    override def expr(
        inputResults: List[RelExprPlanResult]
    ): RelExprPlanResult = RelExprPlanResult(
        RelOpExpr(preparedRelOp, inputResults.map { result => result.relExpr })
    )

    override def dispose(): Unit = opDepsPlans.map { plan => plan.dispose() }
}

private[scleradb]
case class MaterializeRelExprOp(
    processor: Processor,
    tableName: String
) extends RelExprOp {
    private var createdTableIdOpt: Option[TableId] = None

    override def init(): Unit = { /* empty */ }

    override def expr(
        inputResults: List[RelExprPlanResult]
    ): RelExprPlanResult = {
        val tableRef: TableRef = inputResults.head.relExpr match {
            case (tRef: TableRef) => tRef // already materialized -- no-op
            case prepRelExpr => // needs to be materialized
                val createdTable: SchemaTable =
                    processor.createTableFromExpr(
                        prepRelExpr, tableName, Temporary
                    )

                createdTableIdOpt = Some(createdTable.id)
                TableRefSourceExplicit(processor.schema, createdTable)
        }

        RelExprPlanResult(tableRef)
    }

    override def dispose(): Unit =
        createdTableIdOpt.foreach { id => processor.drop(id) }
}

private[scleradb]
abstract class RelEvalOp extends RelPlanOp {
    def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult
}

sealed abstract class RelUnaryEvalOp extends RelEvalOp {
    val opDepsPlans: List[RelExprPlan]

    override def init(): Unit = Plan.cleanInit(opDepsPlans.reverse)

    def opEvalResult(
        inputResult: TableResult,
        resOrder: List[SortExpr]
    ): TableResult

    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val inputResult: TableResult = inputResults.head.tableResult
        val result: TableResult = opEvalResult(inputResult, resOrder)

        if( !SortExpr.isSubsumedBy(resOrder, result.resultOrder) ) {
            throw new RuntimeException(
                "Unexpected sort order: " +
                result.resultOrder.map(s => s.expr.toString).mkString(", ") +
                " - expected: " +
                resOrder.map(s => s.expr.toString).mkString(", ")
            )
        }

        RelEvalPlanResult(result)
    }

    override def dispose(): Unit = opDepsPlans.map { plan => plan.dispose() }
}

private[scleradb]
sealed abstract class RelUnaryBatchEvalOp extends RelUnaryEvalOp {
    val processor: Processor
    val op: RelOp
    assert(op.isLocEvaluable(Location.dataCacheLocation(processor.schema)))

    def partitionInputRows(
        inputRows: Iterator[ScalTableRow]
    ): Iterator[Iterator[ScalTableRow]]

    override def opEvalResult(
        inputResult: TableResult,
        resOrder: List[SortExpr]
    ): TableResult =
        BatchTableResult(
            processor, op, inputResult.columns,
            partitionInputRows(inputResult.typedRows), resOrder
        )
}

private[scleradb]
case class RelUnaryFixedSizeBatchEvalOp(
    override val processor: Processor,
    override val op: RelOp,
    override val opDepsPlans: List[RelExprPlan],
    batchSizeOpt: Option[Int] = None
) extends RelUnaryBatchEvalOp {
    private val batchSize: Int = batchSizeOpt getOrElse ScleraConfig.batchSize

    override def partitionInputRows(
        inputRows: Iterator[ScalTableRow]
    ): Iterator[Iterator[ScalTableRow]] =
        inputRows.grouped(batchSize).map { batch => batch.iterator }
}

private[scleradb]
case class RelUnaryPartitionBatchEvalOp(
    override val processor: Processor,
    override val op: RelOp,
    override val opDepsPlans: List[RelExprPlan],
    partnExprs: List[ScalExpr]
) extends RelUnaryBatchEvalOp {
    override def partitionInputRows(
        inputRows: Iterator[ScalTableRow]
    ): Iterator[Iterator[ScalTableRow]] =
        TableRowGroupIterator(
            processor.scalExprEvaluator, inputRows, partnExprs
        ).map { group => group.rows }
}

private[scleradb]
case class RelProjectEvalOp(
    evaluator: ScalExprEvaluator,
    targetExprs: List[ScalarTarget],
    override val opDepsPlans: List[RelExprPlan]
) extends RelUnaryEvalOp {
    override def opEvalResult(
        inputResult: TableResult,
        resOrder: List[SortExpr]
    ): TableResult =
        ProjectTableResult(evaluator, targetExprs, inputResult, resOrder)
}

private[scleradb]
case class RelSelectEvalOp(
    evaluator: ScalExprEvaluator,
    predExpr: ScalExpr,
    override val opDepsPlans: List[RelExprPlan]
) extends RelUnaryEvalOp {
    override def opEvalResult(
        inputResult: TableResult,
        resOrder: List[SortExpr]
    ): TableResult =
        SelectTableResult(evaluator, predExpr, inputResult)
}

private[scleradb]
case class RelLimitOffsetEvalOp(
    limitOpt: Option[Int],
    offset: Int,
    override val opDepsPlans: List[RelExprPlan]
) extends RelUnaryEvalOp {
    override def opEvalResult(
        inputResult: TableResult,
        resOrder: List[SortExpr]
    ): TableResult =
        LimitOffsetTableResult(limitOpt, offset, inputResult)
}

private[scleradb]
case class RelDistinctEvalOp(
    evaluator: ScalExprEvaluator,
    exprsOpt: Option[List[ScalExpr]],
    override val opDepsPlans: List[RelExprPlan]
) extends RelUnaryEvalOp {
    override def opEvalResult(
        inputResult: TableResult,
        resOrder: List[SortExpr]
    ): TableResult =
        DistinctTableResult(evaluator, exprsOpt, inputResult)
}

private[scleradb]
case class RelEquiMergeJoinEvalOp(
    evaluator: ScalExprEvaluator,
    joinType: JoinType,
    lhsCol: ColRef,
    rhsCol: ColRef
) extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val result: TableResult =
            EquiMergeJoinTableResult(
                evaluator, joinType,
                inputResults(0).tableResult, lhsCol,
                inputResults(1).tableResult, rhsCol
            )

        assert(
            SortExpr.isSubsumedBy(resOrder, result.resultOrder),
            "Unexpected sort order: " +
            result.resultOrder.map(s => s.expr.toString).mkString(", ") +
            " - expected: " +
            resOrder.map(s => s.expr.toString).mkString(", ")
        )

        RelEvalPlanResult(result)
    }
}

// nested loops join with lhs in the outer loop and rhs in the inner loop
private[scleradb]
case class RelEquiNestedLoopsJoinEvalOp(
    processor: Processor,
    joinType: JoinType,
    lhsCol: ColRef,
    rhsCol: ColRef,
    rhsPlan: RelExprPlan
) extends RelEvalOp {
    override def init(): Unit = rhsPlan.init()

    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val result: TableResult =
            EquiNestedLoopsJoinTableResult(
                processor, joinType,
                inputResults.head.tableResult, lhsCol,
                rhsPlan.result.relExpr, rhsCol
            )

        assert(
            SortExpr.isSubsumedBy(resOrder, result.resultOrder),
            "Unexpected sort order: " +
            result.resultOrder.map(s => s.expr.toString).mkString(", ") +
            " - expected: " +
            resOrder.map(s => s.expr.toString).mkString(", ")
        )

        RelEvalPlanResult(result)
    }

    override def dispose(): Unit = rhsPlan.dispose()
}

// evaluate union by merging inputs one after another
private[scleradb]
case object RelUnionEvalOp extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val tableResults: Iterator[TableResult] =
            inputResults.iterator.map { inpResult => inpResult.tableResult }

        val result: TableResult = new TableResult {
            val init: TableResult = tableResults.next()
            override val columns: List[Column] = init.columns

            // normalization ensures that both inputs have the same schema
            override def rows: Iterator[TableRow] =
                init.rows ++ tableResults.flatMap { t => t.rows }

            override val resultOrder: List[SortExpr] = Nil

            override def close(): Unit = { }
        }

        RelEvalPlanResult(result)
    }
}

// sequence alignment
private[scleradb]
case class AlignEvalOp(
    evaluator: ScalExprEvaluator,
    distanceExpr: ScalExpr,
    marginOpt: Option[Int]
) extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val result: TableResult =
            AlignTableResult(
                evaluator,
                distanceExpr,
                marginOpt,
                inputResults.map { inpResult => inpResult.tableResult }
            )

        assert(
            SortExpr.isSubsumedBy(resOrder, result.resultOrder),
            "Unexpected sort order: " +
            result.resultOrder.map(s => s.expr.toString).mkString(", ") +
            " - expected: " +
            resOrder.map(s => s.expr.toString).mkString(", ")
        )

        RelEvalPlanResult(result)
    }
}

// create disjoint intervals from possibly overlapping intervals
private[scleradb]
case class DisjointIntervalEvalOp(
    evaluator: ScalExprEvaluator,
    inpLhsColRef: ColRef,
    inpRhsColRef: ColRef,
    outLhsColRef: ColRef,
    outRhsColRef: ColRef,
    partnColRefs: List[ColRef]
) extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val result: TableResult =
            DisjointIntervalTableResult(
                evaluator,
                inpLhsColRef, inpRhsColRef,
                outLhsColRef, outRhsColRef,
                partnColRefs, inputResults.head.tableResult
            )

        assert(
            SortExpr.isSubsumedBy(resOrder, result.resultOrder),
            "Unexpected sort order: " +
            result.resultOrder.map(s => s.expr.toString).mkString(", ") +
            " - expected: " +
            resOrder.map(s => s.expr.toString).mkString(", ")
        )

        RelEvalPlanResult(result)
    }
}

// unpivot the input table given the specs
private[scleradb]
case class UnPivotEvalOp(
    outValCol: ColRef,
    outKeyCol: ColRef,
    inColVals: List[(ColRef, CharConst)]
) extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val result: TableResult =
            UnPivotTableResult(
                outValCol, outKeyCol, inColVals,
                inputResults.head.tableResult
            )

        assert(
            SortExpr.isSubsumedBy(resOrder, result.resultOrder),
            "Unexpected sort order: " +
            result.resultOrder.map(s => s.expr.toString).mkString(", ") +
            " - expected: " +
            resOrder.map(s => s.expr.toString).mkString(", ")
        )

        RelEvalPlanResult(result)
    }
}

// update the result sort order (unlike Order, no sorting takes place)
private[scleradb]
case class OrderedByEvalOp(
    sortExprs: List[SortExpr]
) extends RelEvalOp {
    override def eval(
        inputResults: List[RelEvalPlanResult],
        resOrder: List[SortExpr]
    ): RelEvalPlanResult = {
        val inputResult: TableResult = inputResults.head.tableResult
        val result: TableResult = new TableResult {
            override val columns: List[Column] = inputResult.columns
            override def rows: Iterator[TableRow] = inputResult.rows
            override val resultOrder: List[SortExpr] = sortExprs
            override def close(): Unit = { }
        }

        assert(
            SortExpr.isSubsumedBy(resOrder, result.resultOrder),
            "Unexpected sort order: " +
            result.resultOrder.map(s => s.expr.toString).mkString(", ") +
            " - expected: " +
            resOrder.map(s => s.expr.toString).mkString(", ")
        )

        RelEvalPlanResult(result)
    }
}
