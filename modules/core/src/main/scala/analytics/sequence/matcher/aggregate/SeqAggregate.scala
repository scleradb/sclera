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

import java.sql.{Time, Timestamp, Date, Blob, Clob}

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.types._
import com.scleradb.sql.expr._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.ScalTableRow
import com.scleradb.sql.exec.ScalTypeEvaluator
import com.scleradb.sql.exec.{ScalCastEvaluator, ScalExprEvaluator}

sealed abstract class SeqAggregate {
    def valueExprs: List[ScalColValue]
    def update(
        evaluator: ScalExprEvaluator,
        row: ScalTableRow,
        rowLabel: Label
    ): SeqAggregate
    val columns: List[Column]

    override def toString: String =
        "[Value = " + valueExprs.map(v => v.repr).mkString(", ") + "]"
}

case class SeqLabeledAggregate(
    aggregate: SeqUnLabeledAggregate,
    labels: List[Label]
) extends SeqAggregate {
    override def valueExprs: List[ScalColValue] = aggregate.valueExprs

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqLabeledAggregate =
        if( labels contains rLabel )
            SeqLabeledAggregate(aggregate.update(evaluator, r, rLabel), labels)
        else this

    override val columns: List[Column] = aggregate.columns
}

sealed abstract class SeqUnLabeledAggregate extends SeqAggregate {
    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqUnLabeledAggregate
}

case class SeqStringAggregate(
    pathCol: ColRef,
    paramOpt: Option[ScalExpr],
    delim: String = ", ",
    path: List[String] = Nil
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] =
        List(CharConst("[" + path.reverse.mkString(delim) + "]"))

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqStringAggregate = {
        val v: String = paramOpt match {
            case Some(param) => evaluator.eval(param, r).repr
            case None => rLabel.id
        }

        SeqStringAggregate(pathCol, paramOpt, delim, v::path)
    }

    override val columns: List[Column] = List(Column(pathCol.name, SqlText))
}

case class SeqIndexFirstAggregate(
    index: Int,
    resultColRef: ColRef,
    param: ScalExpr,
    paramType: SqlType,
    defaultOpt: Option[ScalColValue] = None,
    valueOpt: Option[ScalColValue] = None,
    count: Int = 0
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        valueOpt getOrElse {
            defaultOpt match {
                case Some(default: ScalValueBase) =>
                    ScalCastEvaluator.castScalValueBase(default, paramType)
                case _ => SqlNull(paramType.baseType)
            }
        }
    )

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqIndexFirstAggregate =
        if( count < index )
            SeqIndexFirstAggregate(
                index, resultColRef, param, paramType, defaultOpt,
                valueOpt, count + 1
            )
        else if( count == index )
            SeqIndexFirstAggregate(
                index, resultColRef, param, paramType, defaultOpt,
                Some(evaluator.eval(param, r)), count + 1
            )
        else this

    override val columns: List[Column] =
        List(Column(resultColRef.name, paramType.option))
}

case class SeqIndexLastAggregate(
    index: Int,
    resultColRef: ColRef,
    param: ScalExpr,
    paramType: SqlType,
    defaultOpt: Option[ScalColValue] = None,
    values: List[ScalColValue] = Nil
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        values.drop(index).headOption getOrElse {
            defaultOpt match {
                case Some(default: ScalValueBase) =>
                    ScalCastEvaluator.castScalValueBase(default, paramType)
                case _ => SqlNull(paramType.baseType)
            }
        }
    )

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqIndexLastAggregate =
        SeqIndexLastAggregate(
            index, resultColRef, param, paramType, defaultOpt,
            evaluator.eval(param, r)::(values.take(index))
        )

    override val columns: List[Column] =
        List(Column(resultColRef.name, paramType.option))
}

case class SeqCountAggregate(
    countCol: ColRef,
    paramOpt: Option[ScalExpr],
    count: Long = 0L
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(LongConst(count))

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqCountAggregate = {
        val isIgnore: Boolean = paramOpt match {
            case None => false
            case Some(param) => evaluator.eval(param, r).isNull
        }

        if( isIgnore ) this
        else SeqCountAggregate(countCol, paramOpt, count + 1L)
    }

    override val columns: List[Column] = List(Column(countCol.name, SqlBigInt))
}

case class SeqRankAggregate(
    rankCol: ColRef,
    params: List[ScalExpr],
    isDense: Boolean = false,
    paramVals: List[ScalColValue] = Nil,
    count: Long = 0L,
    rank: Long = 0L
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(LongConst(rank))

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqRankAggregate = {
        val vals: List[ScalColValue] =
            params.map { param => evaluator.eval(param, r) }

        val (nextCount, nextRank) =
            if( paramVals.isEmpty ) (1L, 1L)
            else if( vals == paramVals ) (count + 1L, rank)
            else if( isDense ) (count + 1L, rank + 1L)
            else (count + 1L, count + 1L)

        SeqRankAggregate(rankCol, params, isDense, vals, nextCount, nextRank)
    }

    override val columns: List[Column] = List(Column(rankCol.name, SqlBigInt))
}

case class SeqExistsAggregate(
    existsCol: ColRef,
    exists: Boolean = false
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(BoolConst(exists))

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqExistsAggregate =
        if( exists ) this else SeqExistsAggregate(existsCol, true)

    override val columns: List[Column] = List(Column(existsCol.name, SqlBool))
}

case class SeqBoolAndAggregate(
    boolAndCol: ColRef,
    param: ScalExpr,
    boolAndOpt: Option[Boolean] = None
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        boolAndOpt match {
            case Some(v) => BoolConst(v)
            case None => SqlNull(SqlBool)
        }
    )

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqBoolAndAggregate = {
        val updBoolAndOpt: Option[Boolean] = boolAndOpt match {
            case vOpt@Some(false) => vOpt
            case vOpt =>
                ScalCastEvaluator.valueAsBooleanOpt(
                    evaluator.eval(param, r)
                ) orElse vOpt
        }

        SeqBoolAndAggregate(boolAndCol, param, updBoolAndOpt)
    }

    override val columns: List[Column] =
        List(Column(boolAndCol.name, SqlBool.option))
}

case class SeqBoolOrAggregate(
    boolOrCol: ColRef,
    param: ScalExpr,
    boolOrOpt: Option[Boolean] = None
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        boolOrOpt match {
            case Some(v) => BoolConst(v)
            case None => SqlNull(SqlBool)
        }
    )

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqBoolOrAggregate = {
        val updBoolOrOpt: Option[Boolean] = boolOrOpt match {
            case vOpt@Some(true) => vOpt
            case vOpt =>
                ScalCastEvaluator.valueAsBooleanOpt(
                    evaluator.eval(param, r)
                ) orElse vOpt
        }

        SeqBoolOrAggregate(boolOrCol, param, updBoolOrOpt)
    }

    override val columns: List[Column] =
        List(Column(boolOrCol.name, SqlBool.option))
}

case class SeqOptAggregate(
    resCol: ColRef,
    param: ScalExpr,
    isRunningOpt: (Boolean, ScalValueBase, ScalValueBase) => Boolean,
    sqlType: SqlType,
    runningOpt: Option[ScalValueBase] = None
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        runningOpt match {
            case Some(v) => ScalCastEvaluator.castScalValueBase(v, sqlType)
            case None => SqlNull(sqlType.baseType)
        }
    )

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqOptAggregate = {
        val vOpt: Option[ScalValueBase] =
            evaluator.eval(param, r) match {
                case (_: SqlNull) => None
                case (v: ScalValueBase) => Some(v)
            }

        val isOpt: Boolean = (runningOpt, vOpt) match {
            case (_, None) => false
            case (None, _) => true
            case (Some(m), Some(v)) => isRunningOpt(true, m, v)
        }

        if( isOpt ) SeqOptAggregate(resCol, param, isRunningOpt, sqlType, vOpt)
        else this
    }

    override val columns: List[Column] =
        List(Column(resCol.name, sqlType.option))
}

case class SeqIntSumAggregate(
    sumCol: ColRef,
    param: ScalExpr,
    sumOpt: Option[Long] = None
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        sumOpt match {
            case Some(v) => LongConst(v)
            case None => SqlNull(SqlBigInt)
        }
    )

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqIntSumAggregate = {
        ScalCastEvaluator.valueAsLongOpt(evaluator.eval(param, r)) match {
            case vOpt@Some(v) =>
                sumOpt match {
                    case Some(r) =>
                        SeqIntSumAggregate(sumCol, param, Some(r + v))
                    case None =>
                        SeqIntSumAggregate(sumCol, param, vOpt)
                }

            case None => this
        }
    }

    override val columns: List[Column] =
        List(Column(sumCol.name, SqlBigInt.option))
}

case class SeqFloatSumAggregate(
    sumCol: ColRef,
    param: ScalExpr,
    sumOpt: Option[Double] = None
) extends SeqUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        sumOpt match {
            case Some(v) => DoubleConst(v)
            case None => SqlNull(SqlFloat(None))
        }
    )

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqFloatSumAggregate = {
        ScalCastEvaluator.valueAsDoubleOpt(evaluator.eval(param, r)) match {
            case vOpt@Some(v) =>
                sumOpt match {
                    case Some(r) =>
                        SeqFloatSumAggregate(sumCol, param, Some(r + v))
                    case None =>
                        SeqFloatSumAggregate(sumCol, param, vOpt)
                }

            case None => this
        }
    }

    override val columns: List[Column] =
        List(Column(sumCol.name, SqlFloat(None).option))
}

sealed abstract class SeqPairUnLabeledAggregate extends SeqUnLabeledAggregate {
    val paramY: ScalExpr
    val paramX: ScalExpr

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqPairUnLabeledAggregate =
        ScalCastEvaluator.valueAsDoubleOpt(evaluator.eval(paramY, r)) match {
            case Some(y) =>
                ScalCastEvaluator.valueAsDoubleOpt(
                    evaluator.eval(paramX, r)
                ) match {
                    case Some(x) => update(y, x)
                    case None => this
                }

            case None => this
        }

    def update(y: Double, x: Double): SeqPairUnLabeledAggregate
}

case class SeqCorrAggregate(
    corrCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    sumx: Double = 0.0,
    sumyx: Double = 0.0,
    sumyy: Double = 0.0,
    sumxx: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ey: Double = sumy/count
            val ex: Double = sumx/count
            val eyx: Double = sumyx/count
            val eyy: Double = sumyy/count
            val exx: Double = sumxx/count

            val covaryx: Double = eyx - ey*ex
            val stddevy: Double = scala.math.sqrt(eyy - ey*ey)
            val stddevx: Double = scala.math.sqrt(exx - ex*ex)

            if( stddevy == 0 || stddevx == 0 ) SqlNull(SqlFloat(None)) else {
                val corr: Double = covaryx/(stddevy * stddevx)
                DoubleConst(corr)
            }
        }
    )

    override def update(y: Double, x: Double): SeqCorrAggregate =
        SeqCorrAggregate(
            corrCol, paramY, paramX,
            sumy + y, sumx + x,
            sumyx + y*x, sumyy + y*y, sumxx + x*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(corrCol.name, SqlFloat(None).option))
}

case class SeqCovarAggregate(
    covarCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    isSamp: Boolean,
    sumy: Double = 0.0,
    sumx: Double = 0.0,
    sumyx: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ey: Double = sumy/count
            val ex: Double = sumx/count
            val eyx: Double = sumyx/count

            val covaryx: Double = eyx - ey * ex

            if( isSamp ) {
                if( count == 1 ) SqlNull(SqlFloat(None))
                else DoubleConst(covaryx * count/(count-1))
            } else DoubleConst(covaryx)
        }
    )

    override def update(y: Double, x: Double): SeqCovarAggregate =
        SeqCovarAggregate(
            covarCol, paramY, paramX, isSamp,
            sumy + y, sumx + x,
            sumyx + y*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(covarCol.name, SqlFloat(None).option))
}

case class SeqRegrAvgXAggregate(
    regrAvgXCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumx: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else DoubleConst(sumx/count)
    )

    override def update(y: Double, x: Double): SeqRegrAvgXAggregate =
        SeqRegrAvgXAggregate(
            regrAvgXCol, paramY, paramX,
            sumx + x, count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrAvgXCol.name, SqlFloat(None).option))
}

case class SeqRegrAvgYAggregate(
    regrAvgYCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else DoubleConst(sumy/count)
    )

    override def update(y: Double, x: Double): SeqRegrAvgYAggregate =
        SeqRegrAvgYAggregate(
            regrAvgYCol, paramY, paramX,
            sumy + y, count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrAvgYCol.name, SqlFloat(None).option))
}

case class SeqRegrCountAggregate(
    regrCountCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(LongConst(count))

    override def update(y: Double, x: Double): SeqRegrCountAggregate =
        SeqRegrCountAggregate(
            regrCountCol, paramY, paramX,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrCountCol.name, SqlBigInt))
}

case class SeqRegrSlopeAggregate(
    regrSlopeCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    sumx: Double = 0.0,
    sumyx: Double = 0.0,
    sumxx: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ex: Double = sumx/count
            val exx: Double = sumxx/count

            val varx: Double = exx - ex*ex
            if( varx == 0.0 ) SqlNull(SqlFloat(None)) else {
                val ey: Double = sumy/count
                val eyx: Double = sumyx/count
                val covaryx: Double = eyx - ey*ex

                val regrSlope: Double = covaryx/varx
                DoubleConst(regrSlope)
            }
        }
    )

    override def update(y: Double, x: Double): SeqRegrSlopeAggregate =
        SeqRegrSlopeAggregate(
            regrSlopeCol, paramY, paramX,
            sumy + y, sumx + x,
            sumyx + y*x, sumxx + x*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrSlopeCol.name, SqlFloat(None).option))
}

case class SeqRegrInterceptAggregate(
    regrInterceptCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    sumx: Double = 0.0,
    sumyx: Double = 0.0,
    sumxx: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ex: Double = sumx/count
            val exx: Double = sumxx/count

            val varx: Double = exx - ex*ex
            if( varx == 0.0 ) SqlNull(SqlFloat(None)) else {
                val ey: Double = sumy/count
                val eyx: Double = sumyx/count
                val covaryx: Double = eyx - ey*ex

                val regrIntercept: Double = ey - ex*covaryx/varx
                DoubleConst(regrIntercept)
            }
        }
    )

    override def update(y: Double, x: Double): SeqRegrInterceptAggregate =
        SeqRegrInterceptAggregate(
            regrInterceptCol, paramY, paramX,
            sumy + y, sumx + x,
            sumyx + y*x, sumxx + x*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrInterceptCol.name, SqlFloat(None).option))
}

case class SeqRegrR2Aggregate(
    regrR2Col: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    sumx: Double = 0.0,
    sumyx: Double = 0.0,
    sumyy: Double = 0.0,
    sumxx: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ex: Double = sumx/count
            val exx: Double = sumxx/count

            val varx: Double = exx - ex*ex
            if( varx == 0.0 ) SqlNull(SqlFloat(None)) else {
                val ey: Double = sumy/count
                val eyy: Double = sumyy/count

                val vary: Double = eyy - ey*ey
                if( vary == 0.0 ) DoubleConst(1.0) else {
                    val eyx: Double = sumyx/count
                    val covaryx: Double = eyx - ey*ex

                    val regrR2: Double = (covaryx/vary) * (covaryx/varx)
                    DoubleConst(regrR2)
                }
            }
        }
    )

    override def update(y: Double, x: Double): SeqRegrR2Aggregate =
        SeqRegrR2Aggregate(
            regrR2Col, paramY, paramX,
            sumy + y, sumx + x,
            sumyx + y*x, sumyy + y*y, sumxx + x*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrR2Col.name, SqlFloat(None).option))
}

case class SeqRegrSxxAggregate(
    regrSxxCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumx: Double = 0.0,
    sumxx: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ex: Double = sumx/count

            val regrSxx: Double = sumxx - count * ex * ex
            DoubleConst(regrSxx)
        }
    )

    override def update(y: Double, x: Double): SeqRegrSxxAggregate =
        SeqRegrSxxAggregate(
            regrSxxCol, paramY, paramX,
            sumx + x, sumxx + x*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrSxxCol.name, SqlFloat(None).option))
}

case class SeqRegrSyyAggregate(
    regrSyyCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    sumyy: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ey: Double = sumy/count

            val regrSyy: Double = sumyy - count * ey * ey
            DoubleConst(regrSyy)
        }
    )

    override def update(y: Double, x: Double): SeqRegrSyyAggregate =
        SeqRegrSyyAggregate(
            regrSyyCol, paramY, paramX,
            sumy + y, sumyy + y*y,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrSyyCol.name, SqlFloat(None).option))
}

case class SeqRegrSxyAggregate(
    regrSxyCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    sumx: Double = 0.0,
    sumyx: Double = 0.0,
    count: Long = 0L
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ex: Double = sumx/count
            val ey: Double = sumy/count

            val regrSxy: Double = sumyx - count * ey * ex
            DoubleConst(regrSxy)
        }
    )

    override def update(y: Double, x: Double): SeqRegrSxyAggregate =
        SeqRegrSxyAggregate(
            regrSxyCol, paramY, paramX,
            sumy + y, sumx + x, sumyx + y*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(regrSxyCol.name, SqlFloat(None).option))
}

sealed abstract class SeqSingleUnLabeledAggregate
extends SeqUnLabeledAggregate {
    val param: ScalExpr

    override def update(
        evaluator: ScalExprEvaluator,
        r: ScalTableRow,
        rLabel: Label
    ): SeqSingleUnLabeledAggregate =
        ScalCastEvaluator.valueAsDoubleOpt(evaluator.eval(param, r)) match {
            case Some(x) => update(x)
            case None => this
        }

    def update(x: Double): SeqSingleUnLabeledAggregate
}

case class SeqMovingAvgAggregate(
    avgCol: ColRef,
    override val param: ScalExpr,
    n: Int,
    buffer: List[Double] = Nil
) extends SeqSingleUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( buffer.isEmpty ) SqlNull(SqlFloat(None))
        else DoubleConst(buffer.sum/buffer.size)
    )

    override def update(x: Double): SeqMovingAvgAggregate =
        SeqMovingAvgAggregate(avgCol, param, n, (x::buffer).take(n))

    override val columns: List[Column] =
        List(Column(avgCol.name, SqlFloat(None).option))
}

case class SeqMovingStdDevAggregate(
    varCol: ColRef,
    override val param: ScalExpr,
    n: Int,
    buffer: List[Double] = Nil
) extends SeqSingleUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( buffer.size <= 1 ) SqlNull(SqlFloat(None)) else {
            val count = buffer.size

            val ex: Double = buffer.sum/count
            val exx: Double = buffer.map(v => v*v).sum/count
            val varxpop: Double = exx - ex * ex

            DoubleConst(scala.math.sqrt(varxpop * count/(count-1)))
        }
    )

    override def update(x: Double): SeqMovingStdDevAggregate =
        SeqMovingStdDevAggregate(varCol, param, n, (x::buffer).take(n))

    override val columns: List[Column] =
        List(Column(varCol.name, SqlFloat(None).option))
}

case class SeqExpMovingAvgAggregate(
    avgCol: ColRef,
    override val param: ScalExpr,
    decay: Double,
    avg: Double = 0.0,
    count: Long = 0L
) extends SeqSingleUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0L ) SqlNull(SqlFloat(None)) else DoubleConst(avg)
    )

    override def update(x: Double): SeqExpMovingAvgAggregate = {
        val updAvg: Double =
            if( count == 0L ) x else ((1.0 - decay) * avg + decay * x)
        SeqExpMovingAvgAggregate(avgCol, param, decay, updAvg, count + 1L)
    }

    override val columns: List[Column] =
        List(Column(avgCol.name, SqlFloat(None).option))
}

case class SeqAvgAggregate(
    avgCol: ColRef,
    override val param: ScalExpr,
    avg: Double = 0.0,
    count: Long = 0L
) extends SeqSingleUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0L ) SqlNull(SqlFloat(None)) else DoubleConst(avg)
    )

    override def update(x: Double): SeqAvgAggregate = {
        val nextCount: Long = count + 1L
        SeqAvgAggregate(avgCol, param, avg + (x - avg)/nextCount, nextCount)
    }

    override val columns: List[Column] =
        List(Column(avgCol.name, SqlFloat(None).option))
}

case class SeqAvgStepAggregate(
    avgCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    xStepCum: Double = 0.0
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( xStepCum == 0.0 ) SqlNull(SqlFloat(None)) else {
            val ey: Double = sumy/xStepCum
            DoubleConst(ey)
        }
    )

    override def update(y: Double, x: Double): SeqAvgStepAggregate =
        SeqAvgStepAggregate(
            avgCol, paramY, paramX,
            sumy + x*y,
            xStepCum + x
        )

    override val columns: List[Column] =
        List(Column(avgCol.name, SqlFloat(None).option))
}

case class SeqStdDevAggregate(
    stdDevCol: ColRef,
    override val param: ScalExpr,
    isSamp: Boolean,
    sumx: Double = 0.0,
    sumxx: Double = 0.0,
    count: Long = 0L
) extends SeqSingleUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ex: Double = sumx/count
            val exx: Double = sumxx/count

            val varxpop: Double = exx - ex * ex

            if( isSamp ) {
                if( count <= 1 ) SqlNull(SqlFloat(None))
                else DoubleConst(scala.math.sqrt(varxpop * count/(count-1)))
            } else DoubleConst(scala.math.sqrt(varxpop))
        }
    )

    override def update(x: Double): SeqStdDevAggregate =
        SeqStdDevAggregate(
            stdDevCol, param, isSamp,
            sumx + x, sumxx + x*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(stdDevCol.name, SqlFloat(None).option))
}

case class SeqStdDevStepAggregate(
    stdDevCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    sumyy: Double = 0.0,
    xStepCum: Double = 0.0
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( xStepCum == 0.0 ) SqlNull(SqlFloat(None)) else {
            val ey: Double = sumy/xStepCum
            val eyy: Double = sumyy/xStepCum

            val varypop: Double = eyy - ey * ey
            DoubleConst(scala.math.sqrt(varypop))
        }
    )

    override def update(y: Double, x: Double): SeqStdDevStepAggregate =
        SeqStdDevStepAggregate(
            stdDevCol, paramY, paramX,
            sumy + x*y, sumyy + x*y*y,
            xStepCum + x
        )

    override val columns: List[Column] =
        List(Column(stdDevCol.name, SqlFloat(None).option))
}

case class SeqVarAggregate(
    varCol: ColRef,
    override val param: ScalExpr,
    isSamp: Boolean,
    sumx: Double = 0.0,
    sumxx: Double = 0.0,
    count: Long = 0L
) extends SeqSingleUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val ex: Double = sumx/count
            val exx: Double = sumxx/count

            val varxpop: Double = exx - ex * ex

            if( isSamp ) {
                if( count <= 1 ) SqlNull(SqlFloat(None))
                else DoubleConst(varxpop * count/(count-1))
            } else DoubleConst(varxpop)
        }
    )

    override def update(x: Double): SeqVarAggregate =
        SeqVarAggregate(
            varCol, param, isSamp,
            sumx + x, sumxx + x*x,
            count + 1L
        )

    override val columns: List[Column] =
        List(Column(varCol.name, SqlFloat(None).option))
}

case class SeqVarStepAggregate(
    varCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    sumy: Double = 0.0,
    sumyy: Double = 0.0,
    xStepCum: Double = 0.0
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( xStepCum == 0.0 ) SqlNull(SqlFloat(None)) else {
            val ey: Double = sumy/xStepCum
            val eyy: Double = sumyy/xStepCum

            val varypop: Double = eyy - ey * ey
            DoubleConst(varypop)
        }
    )

    override def update(y: Double, x: Double): SeqVarStepAggregate =
        SeqVarStepAggregate(
            varCol, paramY, paramX,
            sumy + x*y, sumyy + x*y*y,
            xStepCum + x
        )

    override val columns: List[Column] =
        List(Column(varCol.name, SqlFloat(None).option))
}

case class SeqSkewAggregate(
    skewCol: ColRef,
    override val param: ScalExpr,
    isSamp: Boolean,
    ex: Double = 0.0,
    exx: Double = 0.0,
    exxx: Double = 0.0,
    count: Long = 0L
) extends SeqSingleUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val varxpop: Double = exx - ex * ex
            if( varxpop <= 0.0 ) SqlNull(SqlFloat(None)) else {
                val skewxpop: Double =
                    (exxx - 3 * ex * exx + 2 * ex * ex * ex) /
                    scala.math.pow(varxpop, 1.5)

                if( isSamp ) {
                    if( count <= 2 ) SqlNull(SqlFloat(None))
                    else DoubleConst(
                        skewxpop *
                        scala.math.sqrt(count.toDouble*(count-1))/(count-2)
                    )
                } else DoubleConst(skewxpop)
            }
        }
    )

    override def update(x: Double): SeqSkewAggregate = {
        val incrCount: Long = count + 1L
        val incrCountInv: Double = 1.0/incrCount
        val incrCountInvRem: Double = 1.0 - incrCountInv

        SeqSkewAggregate(
            skewCol, param, isSamp,
            incrCountInvRem * ex + incrCountInv * x,
            incrCountInvRem * exx + incrCountInv * x*x,
            incrCountInvRem * exxx + incrCountInv * x*x*x,
            incrCount
        )
    }

    override val columns: List[Column] =
        List(Column(skewCol.name, SqlFloat(None).option))
}

case class SeqSkewStepAggregate(
    skewCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    ey: Double = 0.0,
    eyy: Double = 0.0,
    eyyy: Double = 0.0,
    xStepCum: Double = 0.0
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( xStepCum == 0.0 ) SqlNull(SqlFloat(None)) else {
            val varypop: Double = eyy - ey * ey
            if( varypop <= 0.0 ) SqlNull(SqlFloat(None)) else {
                val skewypop: Double =
                    (eyyy - 3 * ey * eyy + 2 * ey * ey * ey) /
                    scala.math.pow(varypop, 1.5)

                DoubleConst(skewypop)
            }
        }
    )

    override def update(y: Double, x: Double): SeqSkewStepAggregate = {
        val incrxStepCum: Double = xStepCum + x
        val z: Double = x/incrxStepCum
        val zRem: Double = 1.0 - z

        SeqSkewStepAggregate(
            skewCol, paramY, paramX,
            zRem * ey + z * y,
            zRem * eyy + z * y*y,
            zRem * eyyy + z * y*y*y,
            incrxStepCum
        )
    }

    override val columns: List[Column] =
        List(Column(skewCol.name, SqlFloat(None).option))
}

case class SeqKurtosisAggregate(
    kurtosisCol: ColRef,
    override val param: ScalExpr,
    isSamp: Boolean,
    ex: Double = 0.0,
    exx: Double = 0.0,
    exxx: Double = 0.0,
    exxxx: Double = 0.0,
    count: Long = 0L
) extends SeqSingleUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( count == 0 ) SqlNull(SqlFloat(None)) else {
            val varxpop: Double = exx - ex * ex
            val kurtxpop: Double =
                (((exxxx - 4 * ex * exxx + 6 * ex * ex * exx -
                   3 * ex * ex * ex * ex) / varxpop) / varxpop) - 3

            if( isSamp ) {
                if( count <= 3 ) SqlNull(SqlFloat(None))
                else DoubleConst((kurtxpop * (count + 1) + 6) *
                                 (count-1).toDouble/((count-2) * (count-3)))
            } else DoubleConst(kurtxpop)
        }
    )

    override def update(x: Double): SeqKurtosisAggregate = {
        val incrCount: Long = count + 1L
        val incrCountInv: Double = 1.0/incrCount
        val incrCountInvRem: Double = 1.0 - incrCountInv

        SeqKurtosisAggregate(
            kurtosisCol, param, isSamp,
            incrCountInvRem * ex + incrCountInv * x,
            incrCountInvRem * exx + incrCountInv * x*x,
            incrCountInvRem * exxx + incrCountInv * x*x*x,
            incrCountInvRem * exxxx + incrCountInv * x*x*x*x,
            incrCount
        )
    }

    override val columns: List[Column] =
        List(Column(kurtosisCol.name, SqlFloat(None).option))
}

case class SeqKurtosisStepAggregate(
    kurtosisCol: ColRef,
    override val paramY: ScalExpr,
    override val paramX: ScalExpr,
    ey: Double = 0.0,
    eyy: Double = 0.0,
    eyyy: Double = 0.0,
    eyyyy: Double = 0.0,
    xStepCum: Double = 0.0
) extends SeqPairUnLabeledAggregate {
    override def valueExprs: List[ScalColValue] = List(
        if( xStepCum == 0.0 ) SqlNull(SqlFloat(None)) else {
            val varypop: Double = eyy - ey * ey
            if( varypop <= 0.0 ) SqlNull(SqlFloat(None)) else {
                val kurtosisypop: Double =
                    (((eyyyy - 4 * ey * eyyy + 6 * ey * ey * eyy -
                       3 * ey * ey * ey * ey) / varypop) / varypop) - 3

                DoubleConst(kurtosisypop)
            }
        }
    )

    override def update(y: Double, x: Double): SeqKurtosisStepAggregate = {
        val incrxStepCum: Double = xStepCum + x
        val z: Double = x/incrxStepCum
        val zRem: Double = 1.0 - z

        SeqKurtosisStepAggregate(
            kurtosisCol, paramY, paramX,
            zRem * ey + z * y,
            zRem * eyy + z * y*y,
            zRem * eyyy + z * y*y*y,
            zRem * eyyyy + z * y*y*y*y,
            incrxStepCum
        )
    }

    override val columns: List[Column] =
        List(Column(kurtosisCol.name, SqlFloat(None).option))
}

object SeqAggregate {
    def apply(
        functionName: String,
        params: List[ScalExpr],
        inputColumns: List[Column],
        resColRef: ColRef
    ): SeqUnLabeledAggregate = (functionName, params) match {
        case ("EXISTS", Nil) => SeqExistsAggregate(resColRef)

        case ("LAG", List(param, offset: IntegralConst)) =>
            val paramType: SqlType = ScalTypeEvaluator.eval(param, inputColumns)
            val index: Int = offset.integralValue.toInt
            SeqIndexLastAggregate(index, resColRef, param, paramType)

        case ("LAG", List(param,
                          offset: IntegralConst, default: ScalColValue)) =>
            val paramType: SqlType = ScalTypeEvaluator.eval(param, inputColumns)
            val index: Int = offset.integralValue.toInt
            SeqIndexLastAggregate(
                index, resColRef, param, paramType, Some(default)
            )

        case ("NTH_VALUE", List(param, n: IntegralConst)) =>
            val paramType: SqlType = ScalTypeEvaluator.eval(param, inputColumns)
            val index: Int = n.integralValue.toInt - 1
            SeqIndexFirstAggregate(index, resColRef, param, paramType)

        case ("NTH_VALUE", List(param,
                                n: IntegralConst, default: ScalColValue)) =>
            val paramType: SqlType = ScalTypeEvaluator.eval(param, inputColumns)
            val index: Int = n.integralValue.toInt - 1
            SeqIndexFirstAggregate(
                index, resColRef, param, paramType, Some(default)
            )

        case ("PATH", List(CharConst(delim))) =>
            SeqStringAggregate(resColRef, None, delim)

        case ("STRING_AGG", List(param)) =>
            SeqStringAggregate(resColRef, Some(param))
        case ("STRING_AGG", List(param, CharConst(delim))) =>
            SeqStringAggregate(resColRef, Some(param), delim)

        case ("ROW_NUMBER", Nil) => SeqCountAggregate(resColRef, None)

        case ("RANK", params) if !params.isEmpty =>
            SeqRankAggregate(resColRef, params, false)
        case ("DENSE_RANK", params) if !params.isEmpty =>
            SeqRankAggregate(resColRef, params, true)

        case ("COUNT", Nil) => SeqCountAggregate(resColRef, None)
        case ("COUNT", List(param)) => SeqCountAggregate(resColRef, Some(param))

        case ("MOVAVG", List(param, n: IntegralConst)) =>
            SeqMovingAvgAggregate(resColRef, param, n.integralValue.toInt)

        case ("EXPMOVAVG", List(param, decay: NumericConst)) =>
            SeqExpMovingAvgAggregate(resColRef, param, decay.numericValue)

        case ("MOVSTDDEV", List(param, n: IntegralConst)) =>
            SeqMovingStdDevAggregate(resColRef, param, n.integralValue.toInt)

        case ("SUM", List(param)) =>
            ScalTypeEvaluator.eval(param, inputColumns) match {
                case (SqlSmallInt | SqlInteger | SqlBigInt) =>
                    SeqIntSumAggregate(resColRef, param)
                case (_: SqlSinglePrecFloatingPoint |
                      _: SqlDoublePrecFloatingPoint) =>
                    SeqFloatSumAggregate(resColRef, param)
                case t =>
                    throw new IllegalArgumentException(
                        "Function SUM on parameters of type \"" + t.repr +
                        "\" is not supported"
                    )
            }

        case (f@("MIN" | "MAX"), List(param)) =>
            def isRunningOpt(
                isEqualTrue: Boolean,
                cur: ScalValueBase,
                v: ScalValueBase
            ): Boolean = {
                val cmp: Int = cur compare v
                (cmp > 0 && f == "MIN") || (cmp < 0 && f == "MAX") ||
                (cmp == 0 && isEqualTrue)
            }

            val sqlType: SqlType = ScalTypeEvaluator.eval(param, inputColumns)
            SeqOptAggregate(resColRef, param, isRunningOpt, sqlType)

        case ("EVERY", List(param)) =>
            SeqBoolAndAggregate(resColRef, param)
        case ("BOOL_AND", List(param)) =>
            SeqBoolAndAggregate(resColRef, param)
        case ("BOOL_OR", List(param)) =>
            SeqBoolOrAggregate(resColRef, param)

        case ("CORR", List(paramY, paramX)) =>
            SeqCorrAggregate(resColRef, paramY, paramX)

        case ("COVAR_POP", List(paramY, paramX)) =>
            SeqCovarAggregate(resColRef, paramY, paramX, false)
        case ("COVAR_SAMP", List(paramY, paramX)) =>
            SeqCovarAggregate(resColRef, paramY, paramX, true)

        case ("REGR_COUNT", List(paramY, paramX)) =>
            SeqRegrCountAggregate(resColRef, paramY, paramX)
        case ("REGR_AVGX", List(paramY, paramX)) =>
            SeqRegrAvgXAggregate(resColRef, paramY, paramX)
        case ("REGR_AVGY", List(paramY, paramX)) =>
            SeqRegrAvgYAggregate(resColRef, paramY, paramX)
        case ("REGR_SLOPE", List(paramY, paramX)) =>
            SeqRegrSlopeAggregate(resColRef, paramY, paramX)
        case ("REGR_INTERCEPT", List(paramY, paramX)) =>
            SeqRegrInterceptAggregate(resColRef, paramY, paramX)
        case ("REGR_R2", List(paramY, paramX)) =>
            SeqRegrR2Aggregate(resColRef, paramY, paramX)
        case ("REGR_SXX", List(paramY, paramX)) =>
            SeqRegrSxxAggregate(resColRef, paramY, paramX)
        case ("REGR_SYY", List(paramY, paramX)) =>
            SeqRegrSyyAggregate(resColRef, paramY, paramX)
        case ("REGR_SXY", List(paramY, paramX)) =>
            SeqRegrSxyAggregate(resColRef, paramY, paramX)

        case ("AVG", List(param)) =>
            SeqAvgAggregate(resColRef, param)
        case ("AVG", List(param, step)) =>
            SeqAvgStepAggregate(resColRef, param, step)

        case ("STDDEV_POP", List(param)) =>
            SeqStdDevAggregate(resColRef, param, false)
        case ("STDDEV_SAMP" | "STDDEV", List(param)) =>
            SeqStdDevAggregate(resColRef, param, true)
        case ("STDDEV", List(param, step)) =>
            SeqStdDevStepAggregate(resColRef, param, step)

        case ("VAR_POP", List(param)) =>
            SeqVarAggregate(resColRef, param, false)
        case ("VAR_SAMP" | "VAR" | "VARIANCE", List(param)) =>
            SeqVarAggregate(resColRef, param, true)
        case ("VAR" | "VARIANCE", List(param, step)) =>
            SeqVarStepAggregate(resColRef, param, step)

        case ("SKEW_POP", List(param)) =>
            SeqSkewAggregate(resColRef, param, false)
        case ("SKEW_SAMP" | "SKEW", List(param)) =>
            SeqSkewAggregate(resColRef, param, true)
        case ("SKEW", List(param, step)) =>
            SeqSkewStepAggregate(resColRef, param, step)

        case ("KURTOSIS_POP", List(param)) =>
            SeqKurtosisAggregate(resColRef, param, false)
        case ("KURTOSIS_SAMP" | "KURTOSIS", List(param)) =>
            SeqKurtosisAggregate(resColRef, param, true)
        case ("KURTOSIS", List(param, step)) =>
            SeqKurtosisStepAggregate(resColRef, param, step)

        case (rejectFn, rejectInp) =>
            throw new IllegalArgumentException(
                "Function " + rejectFn + " on parameters (" +
                rejectInp.map(p => p.repr).mkString(", ") +
                ") is not supported"
            )
    }

    def apply(
        indexOpt: Option[Int],
        inpCol: Column,
        resultColRef: ColRef
    ): SeqUnLabeledAggregate = indexOpt match {
        case Some(index) if( index >= 0 ) =>
            SeqIndexFirstAggregate(
                index, resultColRef, ColRef(inpCol.name), inpCol.sqlType
            )
        case Some(index) if( index < 0 ) =>
            SeqIndexLastAggregate(
                -index, resultColRef, ColRef(inpCol.name), inpCol.sqlType
            )
        case None =>
            SeqIndexLastAggregate(
                0, resultColRef, ColRef(inpCol.name), inpCol.sqlType
            )
    }

    def apply(
        aggregate: SeqUnLabeledAggregate,
        labels: List[Label]
    ): SeqAggregate = labels match {
        case Nil => aggregate
        case _ => SeqLabeledAggregate(aggregate, labels)
    }
}
