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

package com.scleradb.visual.model.spec

import com.scleradb.sql.expr.{ScalExpr, ScalValueBase, SortExpr}
import com.scleradb.visual.model.plot._

sealed abstract class LayoutSetTask

case class LayoutSetDisplay(tasks: List[DisplaySetTask]) extends LayoutSetTask

case class LayoutSetCoordinates(task: CoordSetTask) extends LayoutSetTask

sealed abstract class DisplaySetTask

case class DisplaySetWidth(width: Double) extends DisplaySetTask

case class DisplaySetHeight(height: Double) extends DisplaySetTask

case class DisplaySetMargin(
    tasks: List[DisplayMarginSetTask]
) extends DisplaySetTask

case class DisplaySetLegend(
    tasks: List[LegendDisplaySetTask]
) extends DisplaySetTask

sealed abstract class DisplayMarginSetTask

case class DisplayMarginSetTop(top: Double) extends DisplayMarginSetTask

case class DisplayMarginSetRight(right: Double) extends DisplayMarginSetTask

case class DisplayMarginSetBottom(bottom: Double) extends DisplayMarginSetTask

case class DisplayMarginSetLeft(left: Double) extends DisplayMarginSetTask

sealed abstract class LegendDisplaySetTask

case class LegendDisplaySetPadding(padding: Double) extends LegendDisplaySetTask

case class LegendDisplaySetWidth(width: Double) extends LegendDisplaySetTask

sealed abstract class CoordSetTask

case class CoordSetGrid(tasks: List[GridSetTask]) extends CoordSetTask

case class CoordSetMap(tasks: List[MapSetTask]) extends CoordSetTask

sealed abstract class GridSetTask

case class GridSetType(gridType: String) extends GridSetTask

case class GridSetAes(tasks: List[GridAesSetTask]) extends GridSetTask

sealed abstract class GridAesSetTask

case class GridAesSetColor(color: String) extends GridAesSetTask

case class GridAesSetAxis(
    axisId: AxisId,
    tasks: List[AxisAesSetTask]
) extends GridAesSetTask

sealed abstract class AxisAesSetTask

case class AxisAesSetColor(color: String) extends AxisAesSetTask

case class AxisAesSetTicks(ticks: String) extends AxisAesSetTask

sealed abstract class MapSetTask

case class MapSetProjection(
    projName: String,
    coordsOpt: Option[(Double, Double)]
) extends MapSetTask

case class MapSetOrientation(x: Double, y: Double, z: Double) extends MapSetTask

sealed abstract class LayerSetTask {
    def scalExprs: List[ScalExpr]
}

case class LayerSetGeom(
    geom: String,
    params: List[(String, ScalExpr)]
) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = params.map { case (_, e) => e }
}

case class LayerSetAes(
    prop: String,
    tasks: List[AesSetTask]
) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = tasks.flatMap { t => t.scalExprs }
}

case class LayerSetGroup(group: ScalExpr) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = List(group)
}

case class LayerSetKey(key: ScalExpr) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = List(key)
}

case class LayerSetPos(
    pos: String,
    params: List[(String, Double)],
    order: List[SortExpr]
) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = order.map { se => se.expr }
}

case class LayerSetStat(
    stat: String,
    params: List[(String, ScalExpr)],
    tasks: List[LayerSetTask]
) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = params.map { case (_, e) => e }
}

case class LayerSetMark(
    axisIdOpt: Option[AxisId],
    predicate: ScalExpr,
    tasks: List[LayerSetTask]
) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] =
        predicate :: tasks.flatMap { task => task.scalExprs }
}

case class LayerSetTooltip(expr: ScalExpr) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = List(expr)
}

case class LayerCopyTooltip(prop: String) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = Nil
}

case class LayerSetHidden(isHidden: Boolean) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = Nil
}

case class LayerSetDisplayOrder(displayOrder: Double) extends LayerSetTask {
    override def scalExprs: List[ScalExpr] = Nil
}

sealed abstract class AesSetTask {
    def scalExprs: List[ScalExpr]
}

case class AesSetValue(expr: ScalExpr) extends AesSetTask {
    override def scalExprs: List[ScalExpr] = List(expr)
}

case class AesCopyValue(prop: String) extends AesSetTask {
    override def scalExprs: List[ScalExpr] = Nil
}

case class AesSetOnNull(onNull: ScalValueBase) extends AesSetTask {
    override def scalExprs: List[ScalExpr] = Nil
}

case class AesSetScale(scale: Scale) extends AesSetTask {
    override def scalExprs: List[ScalExpr] = Nil
}

case object AesSetIdentityScale extends AesSetTask {
    override def scalExprs: List[ScalExpr] = Nil
}

case class AesSetLegend(tasks: List[LegendSetTask]) extends AesSetTask {
    override def scalExprs: List[ScalExpr] = Nil
}

sealed abstract class LegendSetTask

case class LegendSetOrientation(orient: String) extends LegendSetTask

case class LegendSetTitle(label: String) extends LegendSetTask

case class LegendSetLabels(labels: LegendLabels) extends LegendSetTask

case class LegendSetLabelAlign(align: String) extends LegendSetTask

case class LegendSetLabelOrder(isReversed: Boolean) extends LegendSetTask

sealed abstract class TransitionSetTask

case class TransitionSetDuration(duration: Int) extends TransitionSetTask

case class TransitionSetEase(ease: String) extends TransitionSetTask

sealed abstract class DataPlotSetTask

case class DataPlotSetFacet(
    tasks: List[FacetSetTask]
) extends DataPlotSetTask

case class DataPlotSetAxis(
    scalExpr: ScalExpr,
    tasks: List[AxisSetTask]
) extends DataPlotSetTask

case class DataPlotSetSubPlot(
    tasks: List[DataSubPlotSetLayer]
) extends DataPlotSetTask

sealed abstract class DataSubPlotSetTask

case class DataSubPlotSetLayer(
    tasks: List[LayerSetTask]
) extends DataSubPlotSetTask

sealed abstract class AxisSetTask

case class AxisSetLabel(label: String) extends AxisSetTask

case class AxisSetScale(scale: Scale) extends AxisSetTask

case class AxisSetFree(isFree: Boolean) extends AxisSetTask

case class AxisSetIncreasing(isIncreasing: Boolean) extends AxisSetTask

case class AxisSetZoom(isZoom: Boolean) extends AxisSetTask

case class AxisSetWindowSize(sliding: Int) extends AxisSetTask

case class AxisSetTickFormat(format: String) extends AxisSetTask

case class AxisSetTicks(ticks: Int) extends AxisSetTask

case class AxisSetWeight(weight: Double) extends AxisSetTask

sealed abstract class FacetSetTask

case class FacetSetRows(expr: ScalExpr) extends FacetSetTask

case class FacetSetColumns(expr: ScalExpr) extends FacetSetTask

case class FacetSetMargin(margins: Boolean) extends FacetSetTask
