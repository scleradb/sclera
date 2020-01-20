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

import java.text.SimpleDateFormat
import java.util.{Locale, Calendar, TimeZone}
import java.sql.{Time, Timestamp, Date}

import scala.collection.mutable
import scala.jdk.CollectionConverters._

import scala.util.{Random, Try, Success, Failure}
import scala.math.BigDecimal

import com.scleradb.dbms.location.Location

import com.scleradb.sql.expr._
import com.scleradb.sql.types._

private[scleradb]
object ScalFunctionEvaluator {
    import com.scleradb.sql.exec.ScalCastEvaluator._

    val aggregateFunctions: List[String] = List(
        "AVG", "BOOL_AND", "BOOL_OR",
        "CORR", "COUNT", "COVAR_POP", "COVAR_SAMP",
        "EVERY", "EXPMOVAVG", "MAX", "MIN", "NTH_VALUE", "PATH",
        "REGR_COUNT", "REGR_AVGX", "REGR_AVGY", "REGR_INTERCEPT",
        "REGR_R2", "REGR_SLOPE", "REGR_SXX",
        "REGR_SXY", "REGR_SYY",
        "STDDEV", "STDDEV_POP", "STDDEV_SAMP", "STRING_AGG",
        "SKEW", "SKEW_POP", "SKEW_SAMP",
        "KURTOSIS", "KURTOSIS_POP", "KURTOSIS_SAMP",
        "MOVAVG", "STRING_AGG", "SUM",
        "VAR", "VARIANCE", "VAR_POP", "VAR_SAMP"
    )

    abstract class FunctionDef {
        def resultType(inputTypes: List[SqlType]): SqlType
        def eval(params: List[ScalColValue]): ScalColValue
    }

    val scalarFunctions: Map[String, FunctionDef] = Map() ++ List(
        List("ABS") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(x@SqlTypedNull(_: SqlNumeric)) => x
                    case List(ShortConst(v)) =>
                        ShortConst(toShort(toInt(v).abs))
                    case List(IntConst(v)) =>
                        IntConst(toInt(toLong(v).abs))
                    case List(LongConst(v)) =>
                        LongConst(toLong(toBigDecimal(v).abs))
                    case List(FloatConst(v)) =>
                        FloatConst(toFloat(toDouble(v).abs))
                    case List(DoubleConst(v)) =>
                        DoubleConst(toDouble(toBigDecimal(v).abs))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute ABS on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("ROUND") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(x@SqlTypedNull(_: SqlNumeric)) => x
                    case List(x@ShortConst(_)) => x
                    case List(x@IntConst(_)) => x
                    case List(x@LongConst(_)) => x
                    case List(x@FloatConst(v))
                    if( v.isNaN || v.isInfinite ) => x
                    case List(FloatConst(v)) =>
                        FloatConst(
                            toFloat(
                                toBigDecimal(v).round(
                                    BigDecimal.defaultMathContext
                                )
                            )
                        )
                    case List(x@DoubleConst(v))
                    if( v.isNaN || v.isInfinite ) => x
                    case List(DoubleConst(v)) =>
                        DoubleConst(
                            toDouble(
                                toBigDecimal(v).round(
                                    BigDecimal.defaultMathContext
                                )
                            )
                        )
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute ROUND on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("FLOOR") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(x@SqlTypedNull(_: SqlNumeric)) => x
                    case List(x@ShortConst(_)) => x
                    case List(x@IntConst(_)) => x
                    case List(x@LongConst(_)) => x
                    case List(FloatConst(v)) => FloatConst(v.floor)
                    case List(DoubleConst(v)) => DoubleConst(v.floor)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute FLOOR on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("CEIL", "CEILING") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(x@SqlTypedNull(_: SqlNumeric)) => x
                    case List(x@ShortConst(_)) => x
                    case List(x@IntConst(_)) => x
                    case List(x@LongConst(_)) => x
                    case List(FloatConst(v)) => FloatConst(v.ceil)
                    case List(DoubleConst(v)) => DoubleConst(v.ceil)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute CEIL on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TRUNC", "TRUNCATE") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(x@SqlTypedNull(_: SqlNumeric)) => x
                    case List(x@ShortConst(_)) => x
                    case List(x@IntConst(_)) => x
                    case List(x@LongConst(_)) => x
                    case List(FloatConst(v)) =>
                        FloatConst(if( v < 0.0 ) v.ceil else v.floor)
                    case List(DoubleConst(v)) =>
                        DoubleConst(if( v < 0.0 ) v.ceil else v.floor)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TRUNC on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("POWER") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric), _) =>
                        SqlNull(SqlFloat(None))
                    case List(_, SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(x: NumericConst, y: NumericConst) =>
                        DoubleConst(
                            scala.math.pow(x.numericValue, y.numericValue)
                        )
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute POWER on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("SQRT") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(x: NumericConst) =>
                        DoubleConst(scala.math.sqrt(x.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute SQRT on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("SIGN") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlSmallInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlSmallInt)
                    case List(x: NumericConst) =>
                        ShortConst(scala.math.signum(x.numericValue).toShort)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute SIGN on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("DIV") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric), _) =>
                        SqlNull(SqlBigInt)
                    case List(_: NumericConst, SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlBigInt)
                    case List(x: NumericConst, y: NumericConst)
                    if y.numericValue != 0 =>
                        val v: Double = x.numericValue / y.numericValue
                        LongConst(v.toLong)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute DIV on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("MOD") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric), _) =>
                        SqlNull(SqlFloat(None))
                    case List(_: NumericConst, SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(x: NumericConst, y: NumericConst)
                    if y.numericValue != 0 =>
                        val v: Double = x.numericValue / y.numericValue
                        DoubleConst(v - v.toLong)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute MOD on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("EXP") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst) =>
                        DoubleConst(scala.math.exp(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute EXP on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("LOG", "LN") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst)
                    if param.numericValue > 0 =>
                        DoubleConst(scala.math.log(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute LOG on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("LOG10") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst)
                    if param.numericValue > 0 =>
                        DoubleConst(scala.math.log10(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute LOG on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("SIN") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst) =>
                        DoubleConst(scala.math.sin(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute SIN on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("COS") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst) =>
                        DoubleConst(scala.math.cos(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute COS on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TAN") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst) =>
                        DoubleConst(scala.math.tan(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TAN on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("ASIN") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst)
                    if param.numericValue.abs <= 1.0 =>
                        DoubleConst(scala.math.asin(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute ASIN on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("ACOS") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst)
                    if param.numericValue.abs <= 1.0 =>
                        DoubleConst(scala.math.acos(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute ACOS on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("ATAN") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(param: NumericConst) =>
                        DoubleConst(scala.math.atan(param.numericValue))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute ATAN on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("ATAN2") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlNumeric), _) =>
                        SqlNull(SqlFloat(None))
                    case List(_, SqlTypedNull(_: SqlNumeric)) =>
                        SqlNull(SqlFloat(None))
                    case List(y: NumericConst, x: NumericConst) =>
                        DoubleConst(
                            scala.math.atan2(y.numericValue, x.numericValue)
                        )
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute ATAN2 on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("PI") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None)

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil =>
                        DoubleConst(scala.math.Pi)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute PI on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("RANDOM", "RANDUNIFORM") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None)

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil =>
                        DoubleConst(Random.nextDouble())
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute RANDUNIFORM on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("RANDGAUSSIAN", "RANDNORMAL", "WHITENOISE") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None)

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil =>
                        DoubleConst(Random.nextGaussian())
                    case List(dev: NumericConst) if dev.numericValue >= 0 =>
                        DoubleConst(dev.numericValue * Random.nextGaussian())
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute RANDGAUSSIAN on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("RANDOMINT", "RANDINTUNIFORM") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlInteger

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(n: IntegralConst) if n.integralValue > 0  =>
                        IntConst(Random.nextInt(n.integralValue.toInt))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute RANDINTUNIFORM on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("RANDOMSTR") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None)

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(n: IntegralConst) if n.integralValue >= 0 =>
                        val len: Int = n.integralValue.toInt
                        CharConst(Random.alphanumeric.take(len).mkString)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute RANDOMSTR on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("CONCAT") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                if( params.exists { p => p.isNull } )
                    SqlNull(SqlCharVarying(None))
                else {
                    val paramStrs: List[String] = params.map {
                        case CharConst(s) => s
                        case other => other.repr
                    }

                    CharConst(paramStrs.mkString(""))
                }
        },
        List("CHAR_LENGTH", "CHARACTER_LENGTH") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlInteger.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlInteger)
                    case List(CharConst(s)) => IntConst(s.length)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute CHARACTER_LENGTH on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("LOWER") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(s)) => CharConst(s.toLowerCase)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute LOWER on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("UPPER") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(s)) => CharConst(s.toUpperCase)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute UPPER on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TRIM") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(s)) => CharConst(s.trim)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TRIM on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("SUBSTRING") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar), _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(_), SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(s), from: IntegralConst) =>
                        CharConst(
                            s.substring(from.integralValue.toInt - 1)
                        )
                    case List(CharConst(s),
                              from: IntegralConst, n: IntegralConst) =>
                        CharConst(
                            s.substring(
                                from.integralValue.toInt - 1,
                                n.integralValue.toInt
                            )
                        )
                    case List(CharConst(s), CharConst(pat)) =>
                        pat.r.findFirstIn(s) match {
                            case Some(m) => CharConst(m)
                            case None => SqlNull(SqlCharVarying(None))
                        }
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute SUBSTRING on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("STRPOS") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlInteger.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar), _) =>
                        SqlNull(SqlInteger)
                    case List(CharConst(_), SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlInteger)
                    case List(CharConst(s), CharConst(sub)) =>
                        IntConst(s.indexOfSlice(sub) + 1)
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute POSITION on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("REPLACE") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar), _, _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(_), SqlTypedNull(_: SqlChar), _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(_), CharConst(_),
                              SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(s), CharConst(t), CharConst(r)) =>
                        CharConst(s.replace(t, r))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute REPLACE on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("REPLACEALL") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar), _, _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(_), SqlTypedNull(_: SqlChar), _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(_), CharConst(_),
                              SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(s), CharConst(pat), CharConst(r)) =>
                        CharConst(s.replaceAll(pat, r))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute REPLACEALL on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("REPLACEFIRST") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar), _, _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(_), SqlTypedNull(_: SqlChar), _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(_), CharConst(_),
                              SqlTypedNull(_: SqlChar)) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(s), CharConst(pat), CharConst(r)) =>
                        CharConst(s.replaceFirst(pat, r))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute REPLACEALL on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("COALESCE") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case ps@(phead::_) =>
                        ps.find { p => !p.isNull } getOrElse phead
                    case Nil =>
                        throw new IllegalArgumentException(
                            "Cannot compute COALESCE on empty parameter list"
                        )
                }
        },
        List("NULLIF") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(x, y) =>
                        if( x == y ) SqlNull(x.sqlBaseType) else x
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute NULLIF on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("GREATEST") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case phead::ptail =>
                        ptail.foldLeft (phead) {
                            case (prev: ScalValueBase, next: ScalValueBase) =>
                                if( (prev compare next) >= 0 ) prev else next
                            case _ => SqlNull(phead.sqlBaseType)
                        }

                    case Nil => SqlNull()
                }
        },
        List("LEAST") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                inputTypes.headOption getOrElse SqlNull().sqlBaseType.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case phead::ptail =>
                        ptail.foldLeft (phead) {
                            case (prev: ScalValueBase, next: ScalValueBase) =>
                                if( (prev compare next) <= 0 ) prev else next
                            case _ => SqlNull(phead.sqlBaseType)
                        }

                    case Nil => SqlNull()
                }
        },
        List("CURRENT_DATE") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlDate

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil =>
                        val d: java.util.Date = new java.util.Date()
                        DateConst(new java.sql.Date(d.getTime()))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute CURRENT_DATE on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("CURRENT_TIME") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlTime

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil =>
                        val d: java.util.Date = new java.util.Date()
                        TimeConst(new java.sql.Time(d.getTime()))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute CURRENT_TIME on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("NOW", "CURRENT_TIMESTAMP") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlTimestamp

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil =>
                        val d: java.util.Date = new java.util.Date()
                        TimestampConst(new java.sql.Timestamp(d.getTime()))
                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute CURRENT_TIMESTAMP on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("SCALE") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlFloat(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull, _) =>
                        SqlNull(SqlFloat(None))
                    case List(_, _: SqlNull) =>
                        SqlNull(SqlFloat(None))
                    case List(v: NumericConst, scaleVal: IntegralConst) =>
                        val scale: Int = scaleVal.integralValue.toInt
                        val bd: BigDecimal = BigDecimal(v.numericValue)
                        val trunc: BigDecimal =
                            bd.setScale(scale, BigDecimal.RoundingMode.HALF_UP)
                        DoubleConst(trunc.doubleValue)
                }
        },
        List("DATE_PARTSTR") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlCharVarying(None).option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull, _, _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(_, _: SqlNull, _) =>
                        SqlNull(SqlCharVarying(None))
                    case List(_, _, _: SqlNull) =>
                        SqlNull(SqlCharVarying(None))
                    case List(CharConst(styleSpec), CharConst(spec),
                              d: DateTimeConst)
                    if spec.toUpperCase == "QUARTER" =>
                        val cal: Calendar = Calendar.getInstance()
                        cal.setTimeInMillis(d.value.getTime())
                        val qno: Int = cal.get(Calendar.MONTH)/3 + 1
                        styleSpec.toUpperCase match {
                            case "SHORT" => CharConst("Q" + qno)
                            case "LONG" => CharConst("QUARTER" + qno)
                            case _ =>
                                throw new IllegalArgumentException(
                                    "Cannot compute DATE_PARTSTR on " +
                                    "unknown style \"" + styleSpec + "\""
                                )
                        }

                    case List(CharConst(styleSpec), CharConst(spec),
                              d: DateTimeConst) =>
                        val locale: Locale = Locale.getDefault()
                        val style: Int = styleSpec.toUpperCase match {
                            case "SHORT" => Calendar.SHORT
                            case "LONG" => Calendar.LONG
                            case _ =>
                                throw new IllegalArgumentException(
                                    "Cannot compute DATE_PARTSTR on " +
                                    "unknown style \"" + styleSpec + "\""
                                )
                        }
                        val field: Int = spec.toUpperCase match {
                            case "WEEK_OF_MONTH" => Calendar.WEEK_OF_MONTH
                            case "DAY_OF_MONTH" => Calendar.DAY_OF_MONTH
                            case "DAY_OF_WEEK" => Calendar.DAY_OF_WEEK
                            case "DAY_OF_YEAR" => Calendar.DAY_OF_YEAR
                            case "HOUR" => Calendar.HOUR
                            case "MILLISECOND" => Calendar.MILLISECOND
                            case "MINUTE" => Calendar.MINUTE
                            case "MONTH" => Calendar.MONTH
                            case "SECOND" => Calendar.SECOND
                            case "YEAR" => Calendar.YEAR
                            case _ =>
                                throw new IllegalArgumentException(
                                    "Cannot compute DATE_PARTSTR on " +
                                    "unknown specification \"" + spec + "\""
                                )
                        }

                        val cal: Calendar = Calendar.getInstance()
                        cal.setTimeInMillis(d.value.getTime())
                        Option(cal.getDisplayName(field, style, locale)) match {
                            case Some(display) => CharConst(display)
                            case None => CharConst(cal.get(field).toString)
                        }

                    case List(styleVal: CharConst, specVal: CharConst, v) =>
                        val d: ScalColValue =
                            ScalCastEvaluator.castScalColValue(v, SqlDate)
                        eval(List(styleVal, specVal, d))

                    case List(specVal: CharConst, d: DateTimeConst) =>
                        eval(List(CharConst("SHORT"), specVal, d))

                    case List(specVal: CharConst, v) =>
                        val d: ScalColValue =
                            ScalCastEvaluator.castScalColValue(v, SqlDate)
                        eval(List(CharConst("SHORT"), specVal, d))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute DATE_PARTSTR on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("DATE_PART") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlInteger.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(SqlTypedNull(_: SqlChar), _) =>
                        SqlNull(SqlInteger)
                    case List(CharConst(_), (_: SqlNull)) =>
                        SqlNull(SqlInteger)
                    case List(CharConst(spec), d: DateTimeConst)
                    if spec.toUpperCase == "QUARTER" =>
                        val cal: Calendar = Calendar.getInstance()
                        cal.setTimeInMillis(d.value.getTime())
                        IntConst(cal.get(Calendar.MONTH)/3 + 1)

                    case List(CharConst(spec), d: DateTimeConst) =>
                        val field: Int = spec.toUpperCase match {
                            case "WEEK_OF_MONTH" => Calendar.WEEK_OF_MONTH
                            case "DAY_OF_MONTH" => Calendar.DAY_OF_MONTH
                            case "DAY_OF_WEEK" => Calendar.DAY_OF_WEEK
                            case "DAY_OF_YEAR" => Calendar.DAY_OF_YEAR
                            case "HOUR" => Calendar.HOUR
                            case "MILLISECOND" => Calendar.MILLISECOND
                            case "MINUTE" => Calendar.MINUTE
                            case "MONTH" => Calendar.MONTH
                            case "SECOND" => Calendar.SECOND
                            case "YEAR" => Calendar.YEAR
                            case _ =>
                                throw new IllegalArgumentException(
                                    "Cannot compute DATE_PART on " +
                                    "unknown specification \"" + spec + "\""
                                )
                        }

                        val offset: Int =
                            if( field == Calendar.MONTH ) 1 else 0

                        val cal: Calendar = Calendar.getInstance()
                        cal.setTimeInMillis(d.value.getTime())
                        IntConst(cal.get(field) + offset)

                    case List(specVal: CharConst, v) =>
                        val d: ScalColValue =
                            ScalCastEvaluator.castScalColValue(v, SqlDate)
                        eval(List(specVal, d))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute DATE_PART on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TIME_ADD") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlTime.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull, _) => SqlNull(SqlTime)
                    case List(_, (_: SqlNull)) => SqlNull(SqlTime)
                    case List(dInp: ScalValueBase, incrInp: ScalValueBase) =>
                        val d: TimeConst = TimeConst(valueAsTime(dInp))
                        val incr: LongConst = LongConst(valueAsLong(incrInp))

                        d.add(incr.integralValue)

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TIME_ADD on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TIME_DIFF") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull, _) => SqlNull(SqlBigInt)
                    case List(_, (_: SqlNull)) => SqlNull(SqlBigInt)
                    case List(aInp: ScalValueBase, bInp: ScalValueBase) =>
                        val a: TimeConst = TimeConst(valueAsTime(aInp))
                        val b: TimeConst = TimeConst(valueAsTime(bInp))

                        a.subtract(b)

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TIME_DIFF on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("DATE_ADD") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlDate.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull, _) => SqlNull(SqlDate)
                    case List(_, (_: SqlNull)) => SqlNull(SqlDate)
                    case List(dInp: ScalValueBase, incrInp: ScalValueBase) =>
                        val d: DateConst = DateConst(valueAsDate(dInp))
                        val incr: LongConst = LongConst(valueAsLong(incrInp))

                        d.add(incr.integralValue)

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute DATE_ADD on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("DATE_DIFF") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull, _) => SqlNull(SqlBigInt)
                    case List(_, (_: SqlNull)) => SqlNull(SqlBigInt)
                    case List(aInp: ScalValueBase, bInp: ScalValueBase) =>
                        val a: DateConst = DateConst(valueAsDate(aInp))
                        val b: DateConst = DateConst(valueAsDate(bInp))

                        a.subtract(b)

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute DATE_DIFF on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TIMESTAMP_ADD") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlTimestamp.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull, _) => SqlNull(SqlTimestamp)
                    case List(_, (_: SqlNull)) => SqlNull(SqlTimestamp)
                    case List(dInp: ScalValueBase, incrInp: ScalValueBase) =>
                        val d: TimestampConst =
                            TimestampConst(valueAsTimestamp(dInp))
                        val incr: LongConst =
                            LongConst(valueAsLong(incrInp))

                        d.add(incr.integralValue)

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TIMESTAMP_ADD on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TIMESTAMP_DIFF") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull, _) => SqlNull(SqlBigInt)
                    case List(_, (_: SqlNull)) => SqlNull(SqlBigInt)
                    case List(aInp: ScalValueBase, bInp: ScalValueBase) =>
                        val a: TimestampConst =
                            TimestampConst(valueAsTimestamp(aInp))
                        val b: TimestampConst =
                            TimestampConst(valueAsTimestamp(bInp))

                        a.subtract(b)

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TIMESTAMP_DIFF on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TIMESTAMP_UNIX") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case List(_: SqlNull) => SqlNull(SqlBigInt)
                    case List(inp: DateTimeConst) =>
                        LongConst(inp.value.getTime())

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TIMESTAMP_UNIX on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("MILLISECONDS") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil => LongConst(1)
                    case List(_: SqlNull) => SqlNull(SqlBigInt)
                    case List(aInp: ScalValueBase) =>
                        LongConst(valueAsLong(aInp))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute MILLISECONDS on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("SECONDS") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil => LongConst(1000L)
                    case List(_: SqlNull) => SqlNull(SqlBigInt)
                    case List(aInp: ScalValueBase) =>
                        LongConst(1000L * valueAsLong(aInp))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute SECONDS on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("MINUTES") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil => LongConst(60 * 1000L)
                    case List(_: SqlNull) => SqlNull(SqlBigInt)
                    case List(aInp: ScalValueBase) =>
                        LongConst(60 * 1000L * valueAsLong(aInp))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute MINUTES on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("HOURS") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil => LongConst(60 * 60 * 1000L)
                    case List(_: SqlNull) => SqlNull(SqlBigInt)
                    case List(aInp: ScalValueBase) =>
                        LongConst(60 * 60 * 1000L * valueAsLong(aInp))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute HOURS on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("DAYS") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlBigInt.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case Nil => LongConst(24 * 60 * 60 * 1000L)
                    case List(_: SqlNull) => SqlNull(SqlBigInt)
                    case List(aInp: ScalValueBase) =>
                        LongConst(24 * 60 * 60 * 1000L * valueAsLong(aInp))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute DAYS on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TODATE") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlDate.option

            lazy val monthMap: mutable.Map[String, Integer] =
                Calendar.getInstance().getDisplayNames(
                    Calendar.MONTH, Calendar.ALL_STYLES, Locale.US
                ).asScala

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case vs if( vs.exists(v => v.isNull) ) =>
                        SqlNull(SqlDate)

                    case List(CharConst(s), CharConst(format)) =>
                        val df: SimpleDateFormat = new SimpleDateFormat(format)
                        val ts: java.util.Date =
                            try df.parse(s) catch { case (e: Throwable) =>
                                throw new IllegalArgumentException(
                                    e.getMessage(), e
                                )
                            }

                        DateConst(toDate(ts))

                    case List(
                        year: NumericConst,
                        month: ScalValueBase,
                        date: NumericConst
                    ) =>
                        val y: Int = valueAsInt(year)
                        val m: Int = Try(valueAsInt(month)) match {
                            case Success(v) => v - 1
                            case Failure(_) =>
                                val mStr: String = month.value.toString
                                monthMap.get(mStr.trim) match {
                                    case Some(i) => i.toInt
                                    case None =>
                                        throw new IllegalArgumentException(
                                            "Invalid month: \"" + mStr + "\""
                                        )
                                }
                        }
                        val d: Int = valueAsInt(date)

                        val cal: Calendar = Calendar.getInstance()
                        cal.set(y, m, d, 0, 0, 0)
                        DateConst(new Date(cal.getTimeInMillis()))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TODATE on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TOTIME") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlTime.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case vs if( vs.exists(v => v.isNull) ) =>
                        SqlNull(SqlTime)

                    case List(CharConst(s), CharConst(format)) =>
                        val df: SimpleDateFormat = new SimpleDateFormat(format)
                        val ts: java.util.Date =
                            try df.parse(s) catch { case (e: Throwable) =>
                                throw new IllegalArgumentException(
                                    e.getMessage(), e
                                )
                            }

                        TimeConst(toTime(ts))

                    case List(
                        hour: NumericConst,
                        min: NumericConst,
                        sec: NumericConst,
                        msec: NumericConst
                    ) =>
                        val hh: Int = valueAsInt(hour)
                        val mm: Int = valueAsInt(min)
                        val ss: Int = valueAsInt(sec)
                        val ms: Int = valueAsInt(msec)

                        val cal: Calendar = Calendar.getInstance()
                        cal.set(1970, 0, 0, hh, mm, ss)
                        cal.set(Calendar.MILLISECOND, ms)
                        TimeConst(new Time(cal.getTimeInMillis()))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TOTIME on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TOTIMESTAMP") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlTimestamp.option

            lazy val monthMap: mutable.Map[String, Integer] =
                Calendar.getInstance().getDisplayNames(
                    Calendar.MONTH, Calendar.ALL_STYLES, Locale.US
                ).asScala

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case vs if( vs.exists(v => v.isNull) ) =>
                        SqlNull(SqlTimestamp)

                    case List(CharConst(s), CharConst(format)) =>
                        val df: SimpleDateFormat = new SimpleDateFormat(format)
                        val ts: java.util.Date =
                            try df.parse(s) catch { case (e: Throwable) =>
                                throw new IllegalArgumentException(
                                    e.getMessage(), e
                                )
                            }

                        TimestampConst(toTimestamp(ts))

                    case List(
                        year: NumericConst,
                        month: ScalValueBase,
                        date: NumericConst,
                        hour: NumericConst,
                        min: NumericConst,
                        sec: NumericConst,
                        msec: NumericConst
                    ) =>
                        val y: Int = valueAsInt(year)
                        val m: Int = Try(valueAsInt(month)) match {
                            case Success(v) => v - 1
                            case Failure(_) =>
                                val mStr: String = month.value.toString
                                monthMap.get(mStr.trim) match {
                                    case Some(i) => i.toInt
                                    case None =>
                                        throw new IllegalArgumentException(
                                            "Invalid month: \"" + mStr + "\""
                                        )
                                }
                        }
                        val d: Int = valueAsInt(date)
                        val hh: Int = valueAsInt(hour)
                        val mm: Int = valueAsInt(min)
                        val ss: Int = valueAsInt(sec)
                        val ms: Int = valueAsInt(msec)

                        val cal: Calendar = Calendar.getInstance()
                        cal.set(y, m, d, hh, mm, ss)
                        cal.set(Calendar.MILLISECOND, ms)
                        TimestampConst(new Timestamp(cal.getTimeInMillis()))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TOTIMESTAMP on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TOTIMEZONE") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlTimestamp.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case vs if( vs.exists(v => v.isNull) ) =>
                        SqlNull(SqlTimestamp)

                    case List(CharConst(z), TimestampConst(t)) =>
                        val tz: TimeZone = TimeZone.getTimeZone(z)
                        val ms: Long = t.getTime()

                        TimestampConst(new Timestamp(ms + tz.getOffset(ms)))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TOTIMEZONE on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        },
        List("TOUTC") -> new FunctionDef {
            override def resultType(inputTypes: List[SqlType]): SqlType =
                SqlTimestamp.option

            override def eval(params: List[ScalColValue]): ScalColValue =
                params match {
                    case vs if( vs.exists(v => v.isNull) ) =>
                        SqlNull(SqlTimestamp)

                    case List(CharConst(z), TimestampConst(t)) =>
                        val tz: TimeZone = TimeZone.getTimeZone(z)
                        val ms: Long = t.getTime()

                        TimestampConst(new Timestamp(ms - tz.getOffset(ms)))

                    case List(TimestampConst(t)) =>
                        val tz: TimeZone = TimeZone.getDefault()
                        val ms: Long = t.getTime()

                        TimestampConst(new Timestamp(ms - tz.getOffset(ms)))

                    case reject =>
                        throw new IllegalArgumentException(
                            "Cannot compute TOUTC on (" +
                            reject.map(p => p.repr).mkString(", ") + ")"
                        )
                }
        }
    ).flatMap { case (names, fdef) =>
        names.map { name => name.toUpperCase -> fdef }
    }

    def isStreamEvaluable(expr: ScalExpr): Boolean = expr match {
        case ScalOpExpr(ScalarFunction(fname), inputs) =>
            (scalarFunctions contains fname) &&
            inputs.forall { input => isStreamEvaluable(input) }

        case ScalOpExpr(AggregateFunction(fname, FuncAll), inputs) =>
            (aggregateFunctions contains fname) &&
            inputs.forall { input => isStreamEvaluable(input) }

        case ScalOpExpr(AggregateFunction(_, FuncDistinct), _) => false

        case ScalOpExpr(_, inputs) =>
            inputs.forall { input => isStreamEvaluable(input) }

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            isStreamEvaluable(argExpr) &&
            whenThen.forall { case (w, t) =>  
                isStreamEvaluable(w) && isStreamEvaluable(t)
            } &&
            isStreamEvaluable(defaultExpr)

        case _ => true
    }

    def isSupported(loc: Location, expr: ScalExpr): Boolean =
        loc.supportedFunctionsOpt match {
            case Some(supportedFunctions) =>
                isSupported(supportedFunctions, expr)
            case None => true
        }

    private def isSupported(
        supportedFunctions: List[String],
        expr: ScalExpr
    ): Boolean = expr match {
        case ScalOpExpr(f: Function, inputs) =>
            supportedFunctions.contains(f.name) &&
            inputs.forall { input => isSupported(supportedFunctions, input) }

        case ScalOpExpr(_, inputs) =>
            inputs.forall { input => isSupported(supportedFunctions, input) }

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            isSupported(supportedFunctions, argExpr) &&
            whenThen.forall { case (w, t) =>
                isSupported(supportedFunctions, w) &&
                isSupported(supportedFunctions, t)
            } &&
            isSupported(supportedFunctions, defaultExpr)

        case _ => true
    }
}
