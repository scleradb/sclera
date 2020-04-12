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

import com.scleradb.util.tools.Counter

import com.scleradb.dbms.location.{Location, LocationId}
import com.scleradb.exec.{Schema, Processor}

import com.scleradb.objects.Temporary

import com.scleradb.sql.objects.{TableId, SchemaTable}
import com.scleradb.sql.expr._
import com.scleradb.sql.result.TableResult
import com.scleradb.sql.exec.ValuesTableResult

import com.scleradb.plan._

abstract class RelPlan extends Plan {
    val relExpr: RelExpr
    def resultOrder: List[SortExpr] = relExpr.resultOrder
    override def result: RelPlanResult
}

abstract class RelExprPlan extends RelPlan {
    val locationIdOpt: Option[LocationId]
    override def result: RelExprPlanResult
}

case class RelEvalExprPlan(
    processor: Processor,
    override val locationIdOpt: Option[LocationId],
    inputPlan: RelEvalPlan
) extends RelExprPlan {
    val tableName: String = Counter.nextSymbol("SCLERATEMP_")
    val locationId: LocationId =
        locationIdOpt getOrElse Location.dataCacheLocationId
    private val tableId: TableId = TableId(locationId, tableName)

    override lazy val relExpr: RelExpr = {
        val tableRelExpr: RelExpr =
            TableRefSourceById(processor.schema, tableId)

        inputPlan.resultOrder match {
            case Nil => tableRelExpr
            case sortExprs => RelOpExpr(Order(sortExprs), List(tableRelExpr))
        }
    }

    override def init(): Unit = {
        inputPlan.init()

        try {
            val tableResult: TableResult = inputPlan.result.tableResult

            processor.createTableFromTuples(
                tableResult.columns, tableResult.rows,
                locationId, tableName, Temporary
            )
        } catch { case (e: Throwable) =>
            inputPlan.dispose()
            throw e
        }
    }

    override def result: RelExprPlanResult = RelExprPlanResult(relExpr)

    override def dispose(): Unit =
        try processor.drop(tableId) finally inputPlan.dispose()

    override def toString: String =
        "[Location: " + locationId.repr +
        "][Materialize Tuples -> " + tableName +
        "] Evaluate = (" + inputPlan + ")"
}

case class RelBaseExprPlan(
    override val relExpr: RelBaseExpr
) extends RelExprPlan {
    override val locationIdOpt: Option[LocationId] = relExpr.locationIdOpt

    override def init(): Unit = { /* empty */ }

    override def result: RelExprPlanResult = RelExprPlanResult(relExpr)

    override def dispose(): Unit = { /* empty */ }

    override def toString: String = locationIdOpt match {
        case Some(locationId) =>
            "[Location: " + locationId.repr + "][RelBaseExpr = " + relExpr + "]"
        case None =>
            "[Stream][RelBaseExpr = " + relExpr + "]"
    }
}

case class RelOpExprPlan(
    override val relExpr: RelExpr,
    op: RelExprOp,
    inputPlans: List[RelExprPlan]
) extends RelExprPlan {
    override val locationIdOpt: Option[LocationId] = relExpr.locationIdOpt

    override def init(): Unit = {
        Plan.cleanInit(inputPlans.reverse)

        try op.init() catch { case (e: Throwable) =>
            inputPlans.map { plan => plan.dispose() }
            throw e
        }
    }

    override def result: RelExprPlanResult =
        op.expr(inputPlans.map { plan => plan.result })

    override def dispose(): Unit = {
        op.dispose()
        inputPlans.map { plan => plan.dispose() }
    }

    override def toString: String = locationIdOpt match {
        case Some(locationId) =>
            "[Location: " + locationId.repr + "][RelExprOp = " + op +
            "] Inputs = (" + inputPlans.mkString(", ") + ")"
        case None =>
            "[Stream][RelExprOp = " + op +
            "] Inputs = (" + inputPlans.mkString(", ") + ")"
    }
}

abstract class RelEvalPlan extends RelPlan {
    override def result: RelEvalPlanResult
}

case class RelTableResultPlan(
    schema: Schema,
    tableResult: TableResult
) extends RelEvalPlan {
    override val relExpr: RelExpr = ResultValues(schema, tableResult)

    override def init(): Unit = { }

    override def result: RelEvalPlanResult = RelEvalPlanResult(tableResult)

    override def dispose(): Unit = result.close()

    override def toString: String = "[TableResult]"
}

case class RelValuesPlan(
    values: Values
) extends RelEvalPlan {
    override val relExpr: RelExpr = values

    override def init(): Unit = { }

    override def result: RelEvalPlanResult =
        RelEvalPlanResult(ValuesTableResult(values.tableColRefs, values.rows))

    override def dispose(): Unit = result.close()

    override def toString: String = "[Values = " + values + "]"
}

case class RelExprEvalPlan(
    processor: Processor,
    override val relExpr: RelExpr,
    inputPlan: RelExprPlan
) extends RelEvalPlan {
    override def init(): Unit = inputPlan.init()

    private lazy val preparedRelExpr: RelExpr = inputPlan.result.relExpr

    private var resultOpt: Option[RelEvalPlanResult] = None
    override def result: RelEvalPlanResult = resultOpt getOrElse {
        val tableResult: TableResult = processor.queryResult(preparedRelExpr)
        val planResult: RelEvalPlanResult = RelEvalPlanResult(tableResult)

        resultOpt = Some(planResult)
        planResult
    }

    override def dispose(): Unit = {
        resultOpt.foreach { planResult => planResult.close() }
        inputPlan.dispose()
    }

    override def toString: String = "Evaluate = (" + inputPlan + ")"
}

case class RelOpEvalPlan(
    override val relExpr: RelExpr,
    op: RelEvalOp,
    inputPlans: List[RelEvalPlan]
) extends RelEvalPlan {
    override def init(): Unit = {
        Plan.cleanInit(inputPlans.reverse)

        try op.init() catch { case (e: Throwable) =>
            inputPlans.map { plan => plan.dispose() }
            throw e
        }
    }

    private var resultOpt: Option[RelEvalPlanResult] = None
    override def result: RelEvalPlanResult = resultOpt getOrElse {
        val inputResults: List[RelEvalPlanResult] =
            inputPlans.map { input => input.result }

        val planResult: RelEvalPlanResult =
            op.eval(inputResults, relExpr.resultOrder)

        resultOpt = Some(planResult)
        planResult
    }

    override def dispose(): Unit = {
        resultOpt.foreach { planResult => planResult.close() }
        try op.dispose() finally inputPlans.map { input => input.dispose() }
    }

    override def toString: String =
        "[RelEvalOp = " + op + "] Inputs = (" + inputPlans.mkString(", ") + ")"
}
