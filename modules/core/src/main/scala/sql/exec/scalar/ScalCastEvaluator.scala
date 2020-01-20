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

import java.sql.{Time, Timestamp, Date, Blob, Clob}
import java.util.Calendar

import scala.math.ScalaNumericAnyConversions

import com.scleradb.sql.types._
import com.scleradb.sql.expr._

private[scleradb]
object ScalCastEvaluator {
    def castScalColValue(
        cval: ScalColValue,
        sqlType: SqlType
    ): ScalColValue = if( cval.sqlBaseType == sqlType.baseType ) cval else {
        cval match {
            case (_: SqlNull) => SqlNull(sqlType)
            case (v: ScalValueBase) => castScalValueBase(v, sqlType)
        }
    }

    def castScalValueBase(
        cval: ScalValueBase,
        sqlType: SqlType
    ): ScalValueBase = if( cval.sqlBaseType == sqlType.baseType ) cval else {
        sqlType match {
            case SqlSmallInt => ShortConst(valueAsShort(cval))
            case SqlInteger => IntConst(valueAsInt(cval))
            case SqlBigInt => LongConst(valueAsLong(cval))
            case (_: SqlSinglePrecFloatingPoint) =>
                FloatConst(valueAsFloat(cval))
            case (_: SqlDoublePrecFloatingPoint) =>
                DoubleConst(valueAsDouble(cval))
            case SqlBool => BoolConst(valueAsBoolean(cval))
            case (_: SqlChar) => CharConst(valueAsString(cval))
            case SqlTimestamp => TimestampConst(valueAsTimestamp(cval))
            case SqlTime => TimeConst(valueAsTime(cval))
            case SqlDate => DateConst(valueAsDate(cval))
            case SqlBlob => BlobConst(valueAsBlob(cval))
            case SqlClob => ClobConst(valueAsClob(cval))
            case SqlOption(baseType) => castScalValueBase(cval, baseType)
            case t =>
                throw new IllegalArgumentException(
                    "Cannot cast \"" + cval.repr + "\" to \"" + t.repr + "\""
                )
        }
    }

    def valueAsStringOpt(cval: ScalColValue): Option[String] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsString(v))
        }

    def valueAsString(cval: ScalValueBase): String =
        cval match {
            case CharConst(s) => s
            case sv => sv.value.toString
        }

    def valueAsShortOpt(cval: ScalColValue): Option[Short] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsShort(v))
        }

    def valueAsShort(cval: ScalValueBase): Short =
        cval match {
            case ShortConst(v) => v
            case IntConst(v) => toShort(v)
            case LongConst(v) => toShort(v)
            case FloatConst(v) => toShort(v)
            case DoubleConst(v) => toShort(v)
            case CharConst(v) => toShort(v.trim)
            case DateConst(v) => toShort(v.getTime)
            case TimeConst(v) => toShort(v.getTime)
            case TimestampConst(v) => toShort(v.getTime)
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Short"
                )
        }

    def valueAsIntOpt(cval: ScalColValue): Option[Int] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsInt(v))
        }

    def valueAsInt(cval: ScalValueBase): Int =
        cval match {
            case ShortConst(v) => toInt(v)
            case IntConst(v) => v
            case LongConst(v) => toInt(v)
            case FloatConst(v) => toInt(v)
            case DoubleConst(v) => toInt(v)
            case CharConst(v) => toInt(v.trim)
            case DateConst(v) => toInt(v.getTime)
            case TimeConst(v) => toInt(v.getTime)
            case TimestampConst(v) => toInt(v.getTime)
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Int"
                )
        }

    def valueAsLongOpt(cval: ScalColValue): Option[Long] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsLong(v))
        }

    def valueAsLong(cval: ScalValueBase): Long =
        cval match {
            case ShortConst(v) => toLong(v)
            case IntConst(v) => toLong(v)
            case LongConst(v) => v
            case FloatConst(v) => toLong(v)
            case DoubleConst(v) => toLong(v)
            case CharConst(v) => toLong(v.trim)
            case DateConst(v) => toLong(v.getTime)
            case TimeConst(v) => toLong(v.getTime)
            case TimestampConst(v) => toLong(v.getTime)
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Long"
                )
        }

    def valueAsFloatOpt(cval: ScalColValue): Option[Float] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsFloat(v))
        }

    def valueAsFloat(cval: ScalValueBase): Float =
        cval match {
            case ShortConst(v) => toFloat(v)
            case IntConst(v) => toFloat(v)
            case LongConst(v) => toFloat(v)
            case FloatConst(v) => v
            case DoubleConst(v) => toFloat(v)
            case CharConst(v) => toFloat(v.trim)
            case DateConst(v) => toFloat(v.getTime)
            case TimeConst(v) => toFloat(v.getTime)
            case TimestampConst(v) => toFloat(v.getTime)
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Float"
                )
        }

    def valueAsDoubleOpt(cval: ScalColValue): Option[Double] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsDouble(v))
        }

    def valueAsDouble(cval: ScalValueBase): Double =
        cval match {
            case ShortConst(v) => toDouble(v)
            case IntConst(v) => toDouble(v)
            case LongConst(v) => toDouble(v)
            case FloatConst(v) => toDouble(v)
            case DoubleConst(v) => v
            case CharConst(v) => toDouble(v.trim)
            case DateConst(v) => toDouble(v.getTime)
            case TimeConst(v) => toDouble(v.getTime)
            case TimestampConst(v) => toDouble(v.getTime)
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Double"
                )
        }

    def valueAsBooleanOpt(cval: ScalColValue): Option[Boolean] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsBoolean(v))
        }

    def valueAsBoolean(cval: ScalValueBase): Boolean =
        cval match {
            case BoolConst(v) => v
            case CharConst(v) if v.trim.toUpperCase == "TRUE" => true
            case CharConst(v) if v.trim.toUpperCase == "FALSE" => false
            case (v: IntegralConst) => v.integralValue != 0
            case (n: NumericConst) => toLong(n.numericValue) != 0
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Boolean"
                )
        }

    def valueAsDateOpt(cval: ScalColValue): Option[Date] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsDate(v))
        }

    def valueAsDate(cval: ScalValueBase): Date =
        cval match {
            case DateConst(v) => v
            case TimeConst(v) => toDate(v)
            case TimestampConst(v) => toDate(v)
            case CharConst(v) => toDate(v)
            case (n: NumericConst) => toDate(toLong(n.numericValue))
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Date"
                )
        }

    def valueAsTimeOpt(cval: ScalColValue): Option[Time] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsTime(v))
        }

    def valueAsTime(cval: ScalValueBase): Time =
        cval match {
            case DateConst(v) => toTime(v)
            case TimeConst(v) => v
            case TimestampConst(v) => toTime(v)
            case CharConst(v) => toTime(v)
            case (n: NumericConst) => toTime(toLong(n.numericValue))
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Time"
                )
        }

    def valueAsTimestampOpt(cval: ScalColValue): Option[Timestamp] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsTimestamp(v))
        }

    def valueAsTimestamp(cval: ScalValueBase): Timestamp =
        cval match {
            case DateConst(v) => toTimestamp(v)
            case TimeConst(v) => toTimestamp(v)
            case TimestampConst(v) => v
            case CharConst(v) => toTimestamp(v)
            case (n: NumericConst) => toTimestamp(toLong(n.numericValue))
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Timestamp"
                )
        }

    def valueAsBlobOpt(cval: ScalColValue): Option[Blob] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsBlob(v))
        }

    def valueAsBlob(cval: ScalValueBase): Blob =
        cval match {
            case BlobConst(v) => v
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Blob"
                )
        }

    def valueAsClobOpt(cval: ScalColValue): Option[Clob] =
        cval match {
            case (_: SqlNull) => None
            case (v: ScalValueBase) => Some(valueAsClob(v))
        }

    def valueAsClob(cval: ScalValueBase): Clob =
        cval match {
            case ClobConst(v) => v
            case sv =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + sv.repr + "\" to Clob"
                )
        }

    def toShort(v: AnyVal): Short =
        v match {
            case (x: Short) => x
            case (x: Int) if x >= Short.MinValue && x <= Short.MaxValue =>
                x.toShort
            case (x: Long) if x >= Short.MinValue && x <= Short.MaxValue =>
                x.toShort
            case x => toShort(toBigDecimal(x))
        }
    
    def toShort(v: String): Short = toShort(toBigDecimal(v))

    def toShort(v: BigDecimal): Short =
        if( v >= Short.MinValue && v <= Short.MaxValue )
            v.setScale(0, BigDecimal.RoundingMode.HALF_UP).toShort
        else {
            throw new IllegalArgumentException(
                "Value \"" + v.toString +
                "\" is out of range for type SMALLINT"
            )
        }

    def toInt(v: AnyVal): Int =
        v match {
            case (x: Short) => x.toInt
            case (x: Int) => x
            case (x: Long) if x >= Int.MinValue && x <= Int.MaxValue => x.toInt
            case x => toInt(toBigDecimal(x))
        }
    
    def toInt(v: String): Int = toInt(toBigDecimal(v))

    def toInt(v: BigDecimal): Int =
        if( v >= Int.MinValue && v <= Int.MaxValue )
            v.setScale(0, BigDecimal.RoundingMode.HALF_UP).toInt
        else {
            throw new IllegalArgumentException(
                "Value \"" + v.toString +
                "\" is out of range for type INT"
            )
        }

    def toLong(v: AnyVal): Long =
        v match {
            case (x: Short) => x.toLong
            case (x: Int) => x.toLong
            case (x: Long) => x
            case x => toLong(toBigDecimal(x))
        }
    
    def toLong(v: String): Long = toLong(toBigDecimal(v))

    def toLong(v: BigDecimal): Long =
        if( v >= Long.MinValue && v <= Long.MaxValue )
            v.setScale(0, BigDecimal.RoundingMode.HALF_UP).toLong
        else {
            throw new IllegalArgumentException(
                "Value \"" + v.toString +
                "\" is out of range for type BIGINT"
            )
        }

    def toFloat(v: AnyVal): Float =
        v match {
            case (x: Short) => x.toFloat
            case (x: Int) => x.toFloat
            case (x: Long) => x.toFloat
            case (x: Float) => x
            case (x: Double) if( x.isNaN || x.isInfinite ) => x.toFloat
            case (x: Double)
            if x >= Float.MinValue && x <= Float.MaxValue &&
                (x == 0 || x.abs >= Float.MinPositiveValue) => x.toFloat
            case x => toFloat(toBigDecimal(x))
        }
    
    def toFloat(v: String): Float =
        v.trim.toUpperCase match {
            case "NAN" => Float.NaN
            case "INFINITY" => Float.PositiveInfinity
            case "+INFINITY" => Float.PositiveInfinity
            case "-INFINITY" => Float.NegativeInfinity
            case x => toFloat(toBigDecimal(x))
        }

    def toFloat(v: BigDecimal): Float =
        if( v >= Float.MinValue && v <= Float.MaxValue &&
                (v == BigDecimal(0) || v.abs >= Float.MinPositiveValue) )
            v.toFloat
        else throw new IllegalArgumentException(
            "Value \"" + v.toString +
            "\" is out of range for single precision floating point"
        )

    def toDouble(v: AnyVal): Double =
        v match {
            case (x: Short) => x.toDouble
            case (x: Int) => x.toDouble
            case (x: Long) => x.toDouble
            case (x: Float) => x.toDouble
            case (x: Double) => x
            case x => toDouble(toBigDecimal(x))
        }
    
    def toDouble(v: String): Double =
        v.trim.toUpperCase match {
            case "NAN" => Double.NaN
            case "INFINITY" => Double.PositiveInfinity
            case "+INFINITY" => Double.PositiveInfinity
            case "-INFINITY" => Double.NegativeInfinity
            case x => toDouble(toBigDecimal(x))
        }

    def toDouble(v: BigDecimal): Double =
        if( v >= Double.MinValue && v <= Double.MaxValue &&
                (v == BigDecimal(0) || v.abs >= Double.MinPositiveValue) )
            v.toDouble
        else throw new IllegalArgumentException(
            "Value \"" + v.toString +
            "\" is out of range for double precision floating point"
        )

    def toBigDecimal(v: Any): BigDecimal =
        v match {
            case (x: BigDecimal) => x
            case (x: ScalaNumericAnyConversions) => BigDecimal(x.toDouble)
            case x => toBigDecimal(x.toString)
        }

    def toBigDecimal(v: String): BigDecimal =
        try BigDecimal(v) catch {
            case (e: NumberFormatException) =>
                throw new IllegalArgumentException(
                    "Cannot parse as a number: \"" + v + "\"", e
                )
        }

    def toDate(v: java.util.Date): Date =
        v match {
            case (v: Date) => v
            case v =>
                val cal: Calendar = Calendar.getInstance()
                cal.setTime(v)
                cal.set(Calendar.HOUR_OF_DAY, 0)
                cal.set(Calendar.MINUTE, 0)
                cal.set(Calendar.SECOND, 0)
                cal.set(Calendar.MILLISECOND, 0)

                new Date(cal.getTimeInMillis)
        }

    def toDate(v: Long): Date = {
        val cal: Calendar = Calendar.getInstance()
        cal.setTimeInMillis(v)
        cal.set(Calendar.HOUR_OF_DAY, 0)
        cal.set(Calendar.MINUTE, 0)
        cal.set(Calendar.SECOND, 0)
        cal.set(Calendar.MILLISECOND, 0)

        new Date(cal.getTimeInMillis)
    }

    def toDate(v: String): Date =
        try Date.valueOf(v.trim)
        catch {
            case (e: Throwable) =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + v + "\" to Date", e
                )
        }

    def toTime(v: java.util.Date): Time =
        v match {
            case (v: Time) => v
            case v =>
                val cal: Calendar = Calendar.getInstance()
                cal.setTime(v)
                cal.set(Calendar.YEAR, 1970)
                cal.set(Calendar.MONTH, Calendar.JANUARY)
                cal.set(Calendar.DAY_OF_MONTH, 1)

                new Time(cal.getTimeInMillis)
        }

    def toTime(v: Long): Time = new Time(v)

    def toTime(v: String): Time =
        try Time.valueOf(v.trim)
        catch {
            case (e: Throwable) =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + v + "\" to Time", e
                )
        }

    def toTimestamp(v: java.util.Date): Timestamp =
        v match {
            case (v: Timestamp) => v
            case v => new Timestamp(v.getTime)
        }

    def toTimestamp(v: Long): Timestamp = new Timestamp(v)

    def toTimestamp(v: String): Timestamp =
        try Timestamp.valueOf(v.trim)
        catch {
            case (e: Throwable) =>
                throw new IllegalArgumentException(
                    "Cannot translate \"" + v + "\" to Timestamp", e
                )
        }
}

