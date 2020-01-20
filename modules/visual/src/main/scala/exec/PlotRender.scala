/**
* Sclera - Visualization
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

package com.scleradb.visual.exec

import play.api.libs.json.JsObject

import com.scleradb.config.ScleraConfig
import com.scleradb.exec.Processor

import com.scleradb.sql.expr.{ColRef, SortExpr, RelExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.plan.RelEvalPlan
import com.scleradb.sql.result.TableResult

import com.scleradb.visual.model.plot.{Layout, Transition, DataPlot}
import com.scleradb.visual.model.plot.DataPlotInfo

class PlotRender(
    processor: Processor,
    query: RelExpr,
    plotProcessor: PlotProcessor,
    layout: Layout,
    trans: Transition,
    dataPlot: DataPlot,
    isAligned: Boolean
) {
    private val plan: RelEvalPlan = processor.planQuery(query)

    def init(): Unit = plan.init()

    def resultSpecs(batchSize: Int): (JsObject, Iterator[JsObject]) = {
        val result: TableResult = plan.result.tableResult

        val columns: List[Column] = result.columns
        val resultOrder: List[SortExpr] = result.resultOrder

        val dataPlotInfo: DataPlotInfo = PlotPlanner.plan(
            plotProcessor, columns, resultOrder, dataPlot, isAligned
        )

        val specs: JsObject = PlotJson.specifications(
            layout, trans, dataPlot.facetOpt, dataPlotInfo, columns
        )

        val dataIter: Iterator[JsObject] =
            result.rows.grouped(batchSize).map { rows =>
                PlotJson.data(columns, rows)
            }

        (specs, dataIter)
    }

    def close(): Unit = plan.dispose()
}

object PlotRender {
    def apply(
        processor: Processor,
        query: RelExpr,
        plotProcessor: PlotProcessor,
        layout: Layout,
        trans: Transition,
        dataPlot: DataPlot,
        isAligned: Boolean
    ): PlotRender = new PlotRender(
        processor, query, plotProcessor, layout, trans, dataPlot, isAligned
    )
}
