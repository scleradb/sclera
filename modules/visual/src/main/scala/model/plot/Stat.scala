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

package com.scleradb.visual.model.plot

import scala.language.postfixOps
import scala.math.Ordering.Double.TotalOrdering
import scala.collection.mutable

import com.scleradb.util.tools.Counter

import org.apache.commons.math3.exception._
import org.apache.commons.math3.analysis.interpolation.LoessInterpolator

import com.scleradb.sql.exec.ScalCastEvaluator
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.types.{SqlType, SqlInteger, SqlFloat}

import com.scleradb.sql.expr.{RelExpr, RelOpExpr, Project}
import com.scleradb.sql.expr.{ScalExpr, SortExpr, ScalarTarget}
import com.scleradb.sql.expr.{ColRef, ScalColValue, IntConst, DoubleConst}

import com.scleradb.analytics.transform.objects.Transformer
import com.scleradb.analytics.transform.expr._

import com.scleradb.visual.model.spec._
import com.scleradb.visual.exec.{PlotProcessor, PlotNormalizer}

/** Statistics - operator that translates an input layer to an output layer */
sealed abstract class Stat extends Transformer {
    val isAggregate: Boolean

    val statParams: List[(String, ScalExpr)]
    def statLayerTasks: List[LayerSetTask]

    def inpReqOrder(in: Map[String, ScalExpr]): List[SortExpr]

    val outGeomType: String
    val outDispOrder: Double
    val outCols: List[ColRef]

    def scalExprs: List[ScalExpr]

    def rewrite(
        dataExpr: RelExpr,
        facetExprs: List[ScalExpr],
        layer: Layer,
        layerTasks: List[LayerSetTask]
    ): (RelExpr, Layer) = {
        val inGeom: Geom = layer.geom

        val in: Map[String, ScalExpr] = inGeom.propMap ++ statParams
        val group: List[ScalExpr] = layer.groupOpt.toList
        val order: List[SortExpr] = inpReqOrder(in)

        val out: List[(String, ColRef)] =
            outCols.map { col => col.name -> ColRef(Counter.nextSymbol("ST")) }

        val transformType: Transform.TransformType =
            if( isAggregate ) Transform.Union else Transform.Join

        val partnExprs: List[ScalExpr] = facetExprs ::: group
        val outDataExpr: RelExpr = Transform.buildExpr(
            transformType, this, in.toList, partnExprs, order, out, dataExpr
        )

        val outParams: List[(String, ColRef)] =
            out.map { case (name, col) => name -> ColRef(col.name) }
        val outGeom: Geom = Geom(outGeomType, outParams)

        val outLayer: Layer = Layer(
            outGeom, Aes.default(outGeom),
            layer.groupOpt, keyOpt = None, layer.posOpt,
            stats = Nil, marks = Nil, layer.tooltipOpt,
            isHidden = false, isGenerated = true, outDispOrder
        )

        val outLayerTasks: List[LayerSetTask] = statLayerTasks ::: layerTasks
        val outScalExprs: List[ScalExpr] =
            outLayerTasks.flatMap { task => task.scalExprs } filter {
                case (v: ScalColValue) => false
                case _ => true
            } distinct

        val (updOutDataExpr, updOutLayerTasks) =
            if( outScalExprs.isEmpty ) (outDataExpr, outLayerTasks) else {
                val replMap: Map[ColRef, ScalExpr] =
                    (in ++ outParams).map { case (name, expr) =>
                        ColRef(name.toUpperCase) -> expr
                    }

                val outTargetMap: Map[ScalExpr, ColRef] = Map() ++
                    outScalExprs.map {
                        case (col: ColRef) =>
                            val alias: ColRef = replMap.get(col) match {
                                case Some(cref: ColRef) => cref
                                case Some(expr) =>
                                    throw new IllegalArgumentException(
                                        "Found unnormalized expression: \"" +
                                        expr.repr + "\""
                                    )
                                case None =>
                                    throw new IllegalArgumentException(
                                        "Column \"" + col.repr + "\" not found"
                                    )
                            }

                            col -> alias

                        case expr =>
                            expr -> ColRef(Counter.nextSymbol("L"))
                    }

                val outTargets: List[ScalarTarget] =
                    outTargetMap.toList.map { case (expr, col) =>
                        ScalarTarget(ScalExpr.replace(expr, replMap), col)
                    }

                val targets: List[ScalarTarget] =
                    outDataExpr.tableColRefs.map {
                        col => ScalarTarget(col, col)
                    } ::: outTargets

                val isRewriteRedundant: Boolean =
                    targets.forall { t => t.expr == t.alias }

                val statDataExpr: RelExpr =
                    if( isRewriteRedundant ) outDataExpr else {
                        RelOpExpr(Project(targets.distinct), List(outDataExpr))
                    }

                val statLayers: List[LayerSetTask] =
                    outLayerTasks.map { t =>
                        PlotNormalizer.normalize(t, outTargetMap)
                    }

                (statDataExpr, statLayers)
            }

        val updOutLayer: Layer =
            PlotProcessor.applyTasks(outLayer, updOutLayerTasks)

        (updOutDataExpr, updOutLayer)
    }
}

object Stat {
    def bin(
        binWidth: Double,
        minOpt: Option[Double] = None
    ): Bin = {
        if( binWidth <= 0 ) throw new IllegalArgumentException(
            "Bin width must be a positive number"
        )

        Bin(binWidth, minOpt)
    }

    def bin2d(
        xBinWidth: Double,
        yBinWidth: Double,
        xMinOpt: Option[Double] = None,
        yMinOpt: Option[Double] = None
    ): Bin2d = {
        if( xBinWidth <= 0 ) throw new IllegalArgumentException(
            "X axis bin width must be a positive number"
        )

        if( yBinWidth <= 0 ) throw new IllegalArgumentException(
            "Y axis bin width must be a positive number"
        )

        Bin2d(xBinWidth, yBinWidth, xMinOpt, yMinOpt)
    }

    def loessSmooth(
        bandWidthOpt: Option[Double] = None,
        itersOpt: Option[Int] = None,
        accuracyOpt: Option[Double] = None,
        weightExprOpt: Option[ScalExpr] = None
    ): LoessSmooth =
        LoessSmooth(bandWidthOpt, itersOpt, accuracyOpt, weightExprOpt)

    def apply(
        statType: String,
        paramsList: List[(String, ScalExpr)]
    ): Stat = {
        val params: Map[String, ScalExpr] = Map() ++ paramsList

        def exprParamOpt(s: String): Option[ScalExpr] = params.get(s)

        def valParamOpt(s: String): Option[ScalColValue] =
            exprParamOpt(s).map {
                case (v: ScalColValue) => v
                case (e: ScalExpr) =>
                    throw new IllegalArgumentException(
                        "Invalid stat specification: " + statType +
                        "(expecting a value for \"" + s +
                        "\" instead of expression \"" + e.repr + "\")"
                    )
            }

        def doubleParamOpt(s: String): Option[Double] =
            valParamOpt(s).flatMap { e =>
                ScalCastEvaluator.valueAsDoubleOpt(e)
            }

        def doubleParam(s: String): Double =
            doubleParamOpt(s) getOrElse {
                throw new IllegalArgumentException(
                    "Parameter \"" + s + "\" not specified"
                )
            }

        def intParamOpt(s: String): Option[Int] =
            valParamOpt(s).flatMap { e =>
                ScalCastEvaluator.valueAsIntOpt(e)
            }

        statType.toUpperCase match {
            case ("BIN" | "HIST") =>
                bin(doubleParam("BINWIDTH"), doubleParamOpt("MIN"))

            case ("BIN2D" | "HIST2D") => bin2d(
                doubleParamOpt("XBINWIDTH") getOrElse doubleParam("BINWIDTH"),
                doubleParamOpt("YBINWIDTH") getOrElse doubleParam("BINWIDTH"),
                doubleParamOpt("XMIN"),
                doubleParamOpt("YMIN")
            )

            case ("SMOOTH" | "LOESS") => loessSmooth(
                doubleParamOpt("BANDWIDTH"),
                intParamOpt("ITERS"),
                doubleParamOpt("ACCURACY"),
                exprParamOpt("WEIGHT")
            )

            case _ =>
                throw new IllegalArgumentException(
                    "Invalid stat specification: " + statType
                )
        }
    }
}

sealed abstract class StatBin extends Stat {
    override val isAggregate: Boolean = true
    override val outDispOrder: Double = -1
}

case class Bin(
    binWidth: Double,
    minOpt: Option[Double]
) extends StatBin {
    override def scalExprs: List[ScalExpr] = Nil
    override def inpReqOrder(in: Map[String, ScalExpr]): List[SortExpr] = Nil
    override val statParams: List[(String, ScalExpr)] = Nil
    override def statLayerTasks: List[LayerSetTask] = List(
        LayerSetAes("alpha", List(AesSetValue(DoubleConst(1.0)))),
        LayerSetTooltip(ColRef("FREQ"))
    )

    override val outGeomType: String = "HISTOGRAM"
    override val outCols: List[ColRef] =
        List(ColRef("XMIN"), ColRef("XMAX"), ColRef("FREQ"))

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] = {
        val xType: SqlType = inputColTypes("x")
        val xOutType: SqlType = xType.relaxedType
        Map("XMIN" -> xOutType, "XMAX" -> xOutType, "FREQ" -> SqlInteger)
    }

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] = {
        val xType: SqlType = inputColTypes("x")
        val xOutType: SqlType = xType.relaxedType

        val xs: Iterator[Double] = input.flatMap { row =>
            ScalCastEvaluator.valueAsDoubleOpt(row("x"))
        }

        if( xs.isEmpty ) Iterator() else {
            minOpt match {
                case Some(min) =>
                    transform(xs, min, xOutType)

                case None =>
                    val xsList: List[Double] = xs.toList
                    val min: Double = xsList.min

                    transform(xsList.iterator, min, xOutType)
            }
        }
    }

    private def transform(
        xs: Iterator[Double],
        min: Double,
        xOutType: SqlType
    ): Iterator[Map[String, ScalExpr]] = {
        val bins: mutable.Map[Int, Int] = mutable.Map()

        xs.foreach { x =>
            val i: Int = scala.math.floor((x - min) / binWidth).toInt
            bins.get(i) match {
                case Some(n) => bins += i -> (n + 1)
                case None => bins += i -> 1
            }
        }

        bins.iterator.map { case (i, n) =>
            Map(
                "XMIN" -> ScalCastEvaluator.castScalValueBase(
                    DoubleConst(min + i * binWidth), xOutType
                ),
                "XMAX" -> ScalCastEvaluator.castScalValueBase(
                    DoubleConst(min + (i+1) * binWidth), xOutType
                ),
                "FREQ" -> IntConst(n)
            )
        }
    }
}

case class Bin2d(
    xBinWidth: Double,
    yBinWidth: Double,
    xMinOpt: Option[Double],
    yMinOpt: Option[Double]
) extends StatBin {
    override def scalExprs: List[ScalExpr] = Nil
    override def inpReqOrder(in: Map[String, ScalExpr]): List[SortExpr] = Nil
    override val statParams: List[(String, ScalExpr)] = Nil
    override def statLayerTasks: List[LayerSetTask] = List(
        LayerSetAes("alpha", List(AesSetValue(ColRef("NFREQ")))),
        LayerSetTooltip(ColRef("FREQ"))
    )

    override val outGeomType: String = "RECT"
    override val outCols: List[ColRef] = List(
        ColRef("XMIN"), ColRef("XMAX"),
        ColRef("YMIN"), ColRef("YMAX"),
        ColRef("FREQ"), ColRef("NFREQ")
    )

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] = {
        val xType: SqlType = inputColTypes("x")
        val xOutType: SqlType = xType.relaxedType
        val yType: SqlType = inputColTypes("y")
        val yOutType: SqlType = yType.relaxedType

        Map(
            "XMIN" -> xOutType, "XMAX" -> xOutType,
            "YMIN" -> yOutType, "YMAX" -> yOutType,
            "FREQ" -> SqlInteger, "NFREQ" -> SqlFloat(None)
        )
    }

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] = {
        val xType: SqlType = inputColTypes("x")
        val xOutType: SqlType = xType.relaxedType
        val yType: SqlType = inputColTypes("y")
        val yOutType: SqlType = yType.relaxedType

        val inputVals: Iterator[(Double, Double)] = input.flatMap { row =>
            val xOpt: Option[Double] =
                ScalCastEvaluator.valueAsDoubleOpt(row("x"))
            val yOpt: Option[Double] =
                ScalCastEvaluator.valueAsDoubleOpt(row("y"))

            (xOpt, yOpt) match {
                case (Some(x), Some(y)) => Some((x, y))
                case _ => None
            }
        }

        if( inputVals.isEmpty ) Iterator() else {
            val (ps, xMin, yMin) =
                computeMins(inputVals, xMinOpt, yMinOpt)
            transform(ps, xMin, xOutType, yMin, yOutType)
        }
    }

    private def computeMins(
        input: Iterator[(Double, Double)],
        xMinOpt: Option[Double],
        yMinOpt: Option[Double]
    ): (Iterator[(Double, Double)], Double, Double) = {
        val (xInterm, xMin) = computeMin(input, _._1, xMinOpt)

        val (yInterm, yMin) = computeMin(xInterm, _._2, yMinOpt)

        (yInterm, xMin, yMin)
    }

    private def computeMin(
        input: Iterator[(Double, Double)],
        value: ((Double, Double)) => Double,
        minOpt: Option[Double]
    ): (Iterator[(Double, Double)], Double) = minOpt match {
        case Some(min) =>
            (input, min)

        case None =>
            val inputList: List[(Double, Double)] = input.toList
            val min: Double = inputList.map(value).min

            (inputList.iterator, min)
    }

    private def transform(
        ps: Iterator[(Double, Double)],
        xMin: Double,
        xOutType: SqlType,
        yMin: Double,
        yOutType: SqlType
    ): Iterator[Map[String, ScalExpr]] = {
        val bins: mutable.Map[(Int, Int), Int] = mutable.Map()

        ps.foreach { case (x, y) =>
            val xi: Int = scala.math.floor((x - xMin) / xBinWidth).toInt
            val yi: Int = scala.math.floor((y - yMin) / yBinWidth).toInt

            bins.get((xi, yi)) match {
                case Some(n) => bins += (xi, yi) -> (n + 1)
                case None => bins += (xi, yi) -> 1
            }
        }

        val maxBin: Double = bins.values.max.toDouble

        bins.iterator.map { case ((xi, yi), n) =>
            Map(
                "XMIN" -> ScalCastEvaluator.castScalValueBase(
                    DoubleConst(xMin + xi * xBinWidth), xOutType
                ),
                "XMAX" -> ScalCastEvaluator.castScalValueBase(
                    DoubleConst(xMin + (xi+1) * xBinWidth), xOutType
                ),
                "YMIN" -> ScalCastEvaluator.castScalValueBase(
                    DoubleConst(yMin + yi * yBinWidth), yOutType
                ),
                "YMAX" -> ScalCastEvaluator.castScalValueBase(
                    DoubleConst(yMin + (yi+1) * yBinWidth), yOutType
                ),
                "FREQ" -> IntConst(n),
                "NFREQ" -> DoubleConst(n/maxBin)
            )
        }
    }
}

sealed abstract class StatSmooth extends Stat {
    override val isAggregate: Boolean = false
    override val outDispOrder: Double = 1
}

case class LoessSmooth(
    bandWidthOpt: Option[Double],
    itersOpt: Option[Int],
    accuracyOpt: Option[Double],
    weightExprOpt: Option[ScalExpr]
) extends StatSmooth {
    override def scalExprs: List[ScalExpr] = weightExprOpt.toList
    override def inpReqOrder(in: Map[String, ScalExpr]): List[SortExpr] =
        List(SortExpr(in("x")))
    override def statLayerTasks: List[LayerSetTask] = Nil

    private val weightStr: String = "weight"
    override val statParams: List[(String, ScalExpr)] =
        weightExprOpt.toList.map { weight => weightStr -> weight }

    override val outGeomType: String = "LINE"
    override val outCols: List[ColRef] = List(ColRef("X"), ColRef("Y"))

    override def outputColTypes(
        inputColTypes: Map[String, SqlType]
    ): Map[String, SqlType] = {
        val xType: SqlType = inputColTypes("x")
        val yType: SqlType = inputColTypes("y")
        val yOutType: SqlType = yType.relaxedType

        Map("X" -> xType, "Y" -> yOutType)
    }

    override def transform(
        inputColTypes: Map[String, SqlType],
        input: Iterator[Map[String, ScalColValue]]
    ): Iterator[Map[String, ScalExpr]] = {
        val xType: SqlType = inputColTypes("x")
        val yType: SqlType = inputColTypes("y")
        val yOutType: SqlType = yType.relaxedType

        val inputVals: Iterator[(Double, Double, Double)] =
            input.flatMap { row =>
                val xVal: ScalColValue = row("x")
                val yVal: ScalColValue = row("y")
                val wVal: ScalColValue =
                    row.get(weightStr) getOrElse DoubleConst(1)

                val xOpt: Option[Double] =
                    ScalCastEvaluator.valueAsDoubleOpt(xVal)
                val yOpt: Option[Double] =
                    ScalCastEvaluator.valueAsDoubleOpt(yVal)
                val wOpt: Option[Double] =
                    ScalCastEvaluator.valueAsDoubleOpt(wVal)

                if( xOpt.isEmpty || yOpt.isEmpty || wOpt.isEmpty ) None else {
                    Some((xOpt.get, yOpt.get, wOpt.get))
                }
            }

        val (xs, ys, ws) = inputVals.toArray.unzip3

        try {
            val interp: LoessInterpolator = new LoessInterpolator(
                bandWidthOpt getOrElse LoessInterpolator.DEFAULT_BANDWIDTH,
                itersOpt getOrElse LoessInterpolator.DEFAULT_ROBUSTNESS_ITERS,
                accuracyOpt getOrElse LoessInterpolator.DEFAULT_ACCURACY
            )

            val loessVals: Array[Double] = interp.smooth(xs, ys, ws)
            xs.zip(loessVals).iterator.map { case (x, l) =>
                Map(
                    "X" ->
                        ScalCastEvaluator.castScalValueBase(
                            DoubleConst(x), xType
                        ),
                    "Y" ->
                        ScalCastEvaluator.castScalValueBase(
                            DoubleConst(l), yOutType
                        )
                )
            }
        } catch {
            case (e: OutOfRangeException) =>
                throw new IllegalArgumentException(
                    "Out of range: " + e.getMessage(), e
                )
            case (e: NumberIsTooSmallException) =>
                throw new IllegalArgumentException(
                    "Number is too small: " + e.getMessage(), e
                )
        }
    }
}
