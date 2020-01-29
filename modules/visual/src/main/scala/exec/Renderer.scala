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

import com.scleradb.exec.Processor

import com.scleradb.sql.expr.{ColRef, SortExpr, RelExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.plan.RelEvalPlan
import com.scleradb.sql.result.TableResult

import com.scleradb.visual.model.plot.{Plot, DataPlotInfo}
import com.scleradb.visual.model.spec.PlotSpec

sealed abstract class Renderer {
    val processor: Processor
    val dataExpr: RelExpr
    val titleOpt: Option[String]

    private lazy val plan: RelEvalPlan = processor.planQuery(dataExpr)

    def init(): Unit = plan.init()

    def resultSpecs(batchSize: Int): (JsObject, Iterator[JsObject]) = {
        val result: TableResult = plan.result.tableResult
        val specs: JsObject = renderSpecs(result.columns, result.resultOrder)

        val dataIter: Iterator[JsObject] =
            result.rows.grouped(batchSize).map { rows =>
                ResultJson.data(result.columns, rows)
            }

        (specs, dataIter)
    }

    def renderSpecs(
        columns: List[Column],
        resultOrder: List[SortExpr]
    ): JsObject

    def close(): Unit = plan.dispose()
}

class TableRenderer(
    override val processor: Processor,
    override val dataExpr: RelExpr,
    override val titleOpt: Option[String]
) extends Renderer {
    override def renderSpecs(
        columns: List[Column],
        resultOrder: List[SortExpr]
    ): JsObject = ResultJson.table(columns, titleOpt)
}

class PlotRenderer(
    override val processor: Processor,
    plot: Plot,
    override val titleOpt: Option[String]
) extends Renderer {
    override val dataExpr: RelExpr = plot.dataExpr

    override def renderSpecs(
        columns: List[Column],
        resultOrder: List[SortExpr]
    ): JsObject = {
        val dataPlotInfo: DataPlotInfo = PlotPlanner.plan(
            columns, resultOrder, plot.dataPlot, plot.isAligned
        )

        ResultJson.plot(
            plot.layout, plot.transition, plot.dataPlot.facetOpt,
            dataPlotInfo, columns, titleOpt
        )
    }
}

object Renderer {
    def apply(
        processor: Processor,
        dataExpr: RelExpr,
        specOpt: Option[PlotSpec],
        titleOpt: Option[String]
    ): Renderer = specOpt match {
        case Some(spec) => plotRenderer(processor, dataExpr, spec, titleOpt)
        case None => tableRenderer(processor, dataExpr, titleOpt)
    }

    def tableRenderer(
        processor: Processor,
        dataExpr: RelExpr,
        titleOpt: Option[String]
    ): TableRenderer =
        new TableRenderer(processor, dataExpr, titleOpt)

    def plotRenderer(
        processor: Processor,
        dataExpr: RelExpr,
        spec: PlotSpec,
        titleOpt: Option[String]
    ): PlotRenderer = {
        val plot: Plot = PlotProcessor.process(dataExpr, spec)
        plotRenderer(processor, plot, titleOpt)
    }

    def plotRenderer(
        processor: Processor,
        plot: Plot,
        titleOpt: Option[String]
    ): PlotRenderer =
        new PlotRenderer(processor, plot, titleOpt)
}
