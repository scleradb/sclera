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

package com.scleradb.sql.expr

import scala.language.postfixOps

import java.util.Calendar
import java.sql.{Time, Timestamp, Date, Blob, Clob}

import com.scleradb.sql.types._
import com.scleradb.sql.exec.ScalCastEvaluator

import com.scleradb.sql.mapper.default.{PostgreSQLMapper => SqlMapper}

/** Abstract base class for all scalar expressions */
sealed abstract class ScalExpr extends Serializable {
    /** Is this an aggregate scalar expression? */
    def isAggregate: Boolean

    /** Is this an aggregate scalar expression? */
    def isNestedAggregate: Boolean

    /** Sequence aggregates used in this scalar expression */
    def seqAggregates: List[ScalExpr]

    /** Unaggregated columns in this expression */
    def scalarCols: Set[ColRef]

    /** Columns used in this scalar expression */
    def columns: Set[ColRefBase]

    /** Is this expression a column reference? */
    def isColRef: Boolean

    /** Is this expression a constant? */
    def isConstant: Boolean

    /** Column refs used in this scalar expression */
    def colRefs: Set[ColRef] = columns.map {
        case (col: ColRef) => col
        case other => ColRef(other.name)
    }

    /** Replace this expression, or all occurrences of subexpressions */
    def replace(replMap: Map[ScalExpr, ScalExpr]): ScalExpr =
        replMap.get(this) getOrElse replaceSub(replMap)

    /** Replace all occurrences of subexpressions */
    def replaceSub(replMap: Map[ScalExpr, ScalExpr]): ScalExpr

    /** Alias to be used when this expression appears in the SELECT clause */
    def defaultAlias: String = "EXPR"

    /** String representation of this scalar expression */
    def repr: String = SqlMapper.exprString(this)
}

private[scleradb]
object ScalExpr {
    def replace(
        expr: ScalExpr,
        replMap: Map[ColRef, ScalExpr]
    ): ScalExpr = expr match {
        case ScalOpExpr(op, inputs) =>
            val updatedInputs: List[ScalExpr] =
                inputs.map { input => replace(input, replMap) }
            ScalOpExpr(op, updatedInputs)

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            val updatedArgExpr: ScalExpr = replace(argExpr, replMap)
            val updatedWhenThen: List[(ScalExpr, ScalExpr)] = whenThen.map {
                case (w, t) => (replace(w, replMap), replace(t, replMap))
            }

            val updatedDefaultExpr: ScalExpr = replace(defaultExpr, replMap)
            CaseExpr(updatedArgExpr, updatedWhenThen, updatedDefaultExpr)

        case (colRef: ColRef) => replMap(colRef)

        case AnnotColRef(None, name) => replMap(ColRef(name))

        case LabeledColRef(Nil, None, name) => replMap(ColRef(name))

        case other => other
    }
}

// scalar expression tree
private[scleradb]
case class ScalOpExpr(op: ScalOp, inputs: List[ScalExpr]) extends ScalExpr {
    if( op.isAggregate ) inputs.foreach { input =>
        if( input.isAggregate ) throw new IllegalArgumentException(
            "Aggregate expression \"" + input.repr + "\" appears " +
            "within aggregate expression \"" + repr + "\""
        )
    }

    override def isColRef: Boolean = false

    override def isConstant: Boolean = op match {
        case (_: LabeledFunction) => false
        case (_: ScalarFunction) => false // allow non-deterministic functions
        case _ => !op.isAggregate && inputs.forall { input => input.isConstant }
    }

    override def seqAggregates: List[ScalExpr] = op match {
        case (_: LabeledFunction) => List(this)
        case _ => inputs.flatMap { input => input.seqAggregates }
    }

    override def isAggregate: Boolean =
        op.isAggregate || inputs.exists { input => input.isAggregate }

    override def isNestedAggregate: Boolean =
        !op.isAggregate && inputs.exists { input => input.isAggregate }

    override def scalarCols: Set[ColRef] =
        if( op.isAggregate ) Set() else {
            inputs.flatMap { input => input.scalarCols } toSet
        }

    override def columns: Set[ColRefBase] =
        inputs.flatMap { input => input.columns } toSet

    override def replaceSub(replMap: Map[ScalExpr, ScalExpr]): ScalExpr = {
        val updatedInputs: List[ScalExpr] =
            inputs.map { input => input.replace(replMap) }
        ScalOpExpr(op, updatedInputs)
    }

    override def defaultAlias: String = op.defaultAlias(inputs)
}

// case expression
private[scleradb]
case class CaseExpr(
    argExpr: ScalExpr,
    whenThen: List[(ScalExpr, ScalExpr)],
    defaultExpr: ScalExpr
) extends ScalExpr {
    private val exprs: List[ScalExpr] = {
        val (whenExprs, thenExprs) = whenThen.unzip
        argExpr::defaultExpr::(whenExprs:::thenExprs)
    }

    override def isColRef: Boolean = false

    override def isConstant: Boolean =
        exprs.forall { expr => expr.isConstant }

    override def seqAggregates: List[ScalExpr] =
        exprs.flatMap { expr => expr.seqAggregates }

    override def isAggregate: Boolean =
        exprs.exists { expr => expr.isAggregate }

    override def isNestedAggregate: Boolean = isAggregate

    override def scalarCols: Set[ColRef] =
        exprs.flatMap { expr => expr.scalarCols } toSet

    override def columns: Set[ColRefBase] =
        exprs.flatMap { expr => expr.columns } toSet

    override def replaceSub(replMap: Map[ScalExpr, ScalExpr]): ScalExpr = {
        val updatedArgExpr: ScalExpr = argExpr.replace(replMap)
        val updatedWhenThen: List[(ScalExpr, ScalExpr)] = whenThen.map {
            case (w, t) => (w.replace(replMap), t.replace(replMap))
        }
        val updatedDefaultExpr: ScalExpr = defaultExpr.replace(replMap)

        CaseExpr(updatedArgExpr, updatedWhenThen, updatedDefaultExpr)
    }

    override def defaultAlias: String = "CASE"
}

/** Abstract base class for base scalar expressions */
sealed abstract class ScalBaseExpr extends ScalExpr {
    override def replaceSub(replMap: Map[ScalExpr, ScalExpr]): ScalExpr = this
    override def isAggregate: Boolean = false
    override def isNestedAggregate: Boolean = false
}

/** Column reference base */
sealed abstract class ColRefBase extends ScalBaseExpr {
    def labels: List[String]
    val name: String

    override def isConstant: Boolean = false

    override def columns: Set[ColRefBase] = Set(this)
    override def scalarCols: Set[ColRef] = colRefs

    override def defaultAlias: String = name
}

/** Column reference
  *
  * @param name Name of the column
  */
case class ColRef(
    override val name: String
) extends ColRefBase {
    override def isColRef: Boolean = true
    override def labels: List[String] = Nil
    override def seqAggregates: List[ScalExpr] = Nil
}

/** Annotated column reference
  *
  * @param tableNameOpt Name of the table associated with the column
  * @param name         Name of the column
  */
case class AnnotColRef(
    tableNameOpt: Option[String],
    override val name: String
) extends ColRefBase {
    override def isColRef: Boolean = false
    /** Change the case of the table and column name to upper */
    def toUpperCase: AnnotColRef =
        AnnotColRef(tableNameOpt.map(tn => tn.toUpperCase), name.toUpperCase)
    override def labels: List[String] = tableNameOpt.toList
    override def seqAggregates: List[ScalExpr] =
        if( tableNameOpt.isEmpty ) Nil else List(this)
}

/** Labeled, indexed column reference
  *
  * @param labels   Labels associated with the column
  * @param indexOpt Index associated with the column
  * @param name     Name of the column
  */
private[scleradb]
case class LabeledColRef(
    override val labels: List[String],
    indexOpt: Option[Int],
    override val name: String
) extends ColRefBase {
    override def isColRef: Boolean = false
    override def seqAggregates: List[ScalExpr] =
        if( labels.isEmpty && indexOpt.isEmpty ) Nil else List(this)
}

/** Scalar valued expression */
sealed abstract class ScalValue extends ScalBaseExpr {
    override def isColRef: Boolean = false
    override def isConstant: Boolean = true

    override def seqAggregates: List[ScalExpr] = Nil
    override def scalarCols: Set[ColRef] = Set()

    override def columns: Set[ColRefBase] = Set()
    override def defaultAlias: String = "COLUMN"
}

/** Scalar valued expressions that can appear as column values */
sealed abstract class ScalColValue extends ScalValue {
    /** Type of the scalar value */
    val sqlBaseType: SqlType
    /** Is this a NULL? */
    def isNull: Boolean
    /** Cover value - used when comparing across constants of different types */
    def coverValue: Option[Any]
}

/** Scalar valued constants that can appear as non-NULL column values */
sealed abstract class ScalValueBase extends
                      ScalColValue with Ordered[ScalValueBase] {
    /** Scalar value */
    val value: Any
    /** Comparison */
    override def compare(that: ScalValueBase): Int

    override def isNull: Boolean = false
    override def defaultAlias: String = "VALUE"

    override def coverValue: Some[Any]
}

/** Numeric non-null scalar valued constants */
sealed abstract class NumericConst extends ScalValueBase {
    def numericValue: Double
}

/** Integral non-null scalar valued constants */
sealed abstract class IntegralConst extends NumericConst {
    def integralValue: Long
    override def coverValue: Some[BigDecimal] = Some(BigDecimal(integralValue))

    override def compare(that: ScalValueBase): Int =
        integralValue compare ScalCastEvaluator.valueAsLong(that)
}

/** Short constant */
case class ShortConst(
    override val value: Short
) extends IntegralConst {
    override def numericValue: Double = value.toDouble
    override def integralValue: Long = value.toLong

    override val sqlBaseType: SqlBaseType[Short] = SqlSmallInt
}

/** Integer constant */
case class IntConst(
    override val value: Int
) extends IntegralConst {
    override def numericValue: Double = value.toDouble
    override def integralValue: Long = value.toLong

    override val sqlBaseType: SqlBaseType[Int] = SqlInteger
}

/** Long constant */
case class LongConst(
    override val value: Long
) extends IntegralConst {
    override def numericValue: Double = value.toDouble
    override def integralValue: Long = value

    override val sqlBaseType: SqlBaseType[Long] = SqlBigInt
}

/** Floating point non-null scalar valued constants */
sealed abstract class FloatingPointConst extends NumericConst {
    override def compare(that: ScalValueBase): Int =
        numericValue compare ScalCastEvaluator.valueAsDouble(that)
    override def coverValue: Some[BigDecimal] = Some(BigDecimal(numericValue))
}

/** Float constant */
case class FloatConst(
    override val value: Float
) extends FloatingPointConst {
    override def numericValue: Double = value.toDouble

    override val sqlBaseType: SqlBaseType[Float] = SqlReal
}

/** Double constant */
case class DoubleConst(
    override val value: Double
) extends FloatingPointConst {
    override def numericValue: Double = value

    override val sqlBaseType: SqlBaseType[Double] = SqlFloat(None)
}

/** Boolean constant */
case class BoolConst(
    override val value: Boolean
) extends ScalValueBase {
    override val sqlBaseType: SqlBaseType[Boolean] = SqlBool
    override def compare(that: ScalValueBase): Int =
        value compare ScalCastEvaluator.valueAsBoolean(that)

    override def coverValue: Some[Boolean] = Some(value)
}

/** Character string constant */
case class CharConst(
    override val value: String
) extends ScalValueBase {
    override val sqlBaseType: SqlBaseType[String] = SqlCharVarying(None)
    override def compare(that: ScalValueBase): Int =
        value compare ScalCastEvaluator.valueAsString(that)

    override def coverValue: Some[String] = Some(value)

    // override to avoid the cast
    override def repr: String = "'" + value.replaceAll("'", "''") + "'"
}

/** Date/Time constants */
sealed abstract class DateTimeConst extends ScalValueBase {
    override val value: java.util.Date

    def subtract(other: DateTimeConst): LongConst =
        LongConst(value.getTime() - other.value.getTime())

    def add(delta: Long): DateTimeConst
}

/** Date constant */
case class DateConst(
    override val value: Date
) extends DateTimeConst {
    override val sqlBaseType: SqlBaseType[Date] = SqlDate
    override def compare(that: ScalValueBase): Int =
        value compareTo ScalCastEvaluator.valueAsDate(that)

    override def coverValue: Some[Date] = Some(value)

    override def add(delta: Long): DateConst = {
        val cal: Calendar = Calendar.getInstance()
        cal.setTime(value)
        cal.add(Calendar.DATE, delta.toInt)
        DateConst(new Date(cal.getTimeInMillis))
    }
}

/** Time constant */
case class TimeConst(
    override val value: Time
) extends DateTimeConst {
    override val sqlBaseType: SqlBaseType[Time] = SqlTime
    override def compare(that: ScalValueBase): Int =
        value compareTo ScalCastEvaluator.valueAsTime(that)

    override def coverValue: Some[Time] = Some(value)

    override def add(delta: Long): TimeConst =
        TimeConst(new Time(value.getTime + delta))
}

/** Timestamp constant */
case class TimestampConst(
    override val value: Timestamp
) extends DateTimeConst {
    override val sqlBaseType: SqlBaseType[Timestamp] = SqlTimestamp
    override def compare(that: ScalValueBase): Int =
        value compareTo ScalCastEvaluator.valueAsTimestamp(that)

    override def coverValue: Some[Timestamp] = Some(value)

    override def add(delta: Long): TimestampConst =
        TimestampConst(new Timestamp(value.getTime + delta))
}

/** LOB constants */
sealed abstract class LobConst extends ScalValueBase

/** Blob constant */
case class BlobConst(
    override val value: Blob
) extends LobConst {
    override val sqlBaseType: SqlBaseType[Blob] = SqlBlob
    override def compare(that: ScalValueBase): Int =
        if( value == ScalCastEvaluator.valueAsBlob(that) ) 0
        else throw new IllegalArgumentException(
            "Cannot compare BLOB values"
        )

    override def coverValue: Some[Blob] = Some(value)
}

/** Clob constant */
case class ClobConst(
    override val value: Clob
) extends LobConst {
    override val sqlBaseType: SqlBaseType[Clob] = SqlClob
    override def compare(that: ScalValueBase): Int =
        if( value == ScalCastEvaluator.valueAsClob(that) ) 0
        else throw new IllegalArgumentException(
            "Cannot compare CLOB values"
        )

    override def coverValue: Some[Clob] = Some(value)
}

/** SQL NULL */
sealed abstract class SqlNull extends ScalColValue

case class SqlTypedNull(
    override val sqlBaseType: SqlType
) extends SqlNull {
    override def isNull: Boolean = true
    override def defaultAlias: String = "NULL"
    override def coverValue: Option[Any] = None
}

object SqlNull {
    def apply(t: SqlType): SqlNull = new SqlTypedNull(t.baseType)
    def apply(): SqlNull = new SqlTypedNull(SqlCharVarying(None))
}

/** Row of a table, specified as a list of scalar values */
case class Row(scalars: List[ScalColValue]) extends ScalValue {
    override def defaultAlias: String = "ROW"
}

// pattern string, with associated escape sequence
private[scleradb]
case class Pattern(pattern: String, esc: String) extends ScalValue {
    override def defaultAlias: String = "PATTERN"
}

// subqueries
private[scleradb]
sealed abstract class ScalSubQueryBase extends ScalBaseExpr {
    override def isColRef: Boolean = false
    override def isConstant: Boolean = false // accomodating for correlations
    override def seqAggregates: List[ScalExpr] = Nil
    override def scalarCols: Set[ColRef] = Set()
}

// cast to scalar
// TODO: support correlated subQueries
private[scleradb]
case class ScalSubQuery(relExpr: RelExpr) extends ScalSubQueryBase {
    override def columns: Set[ColRefBase] = Set()
    override def defaultAlias: String = "SUBQUERY"
}

// TODO: support correlated subQueries
private[scleradb]
case class Exists(relExpr: RelExpr) extends ScalSubQueryBase {
    override def columns: Set[ColRefBase] = Set()
    override def defaultAlias: String = "EXISTS"
}

// relational side of a scalar/relational comparison
// TODO: support correlated subQueries
private[scleradb]
case class ScalCmpRelExpr(
    qual: CmpQual,
    subQueryOrList: RelSubQueryBase
) extends ScalSubQueryBase {
    override def columns: Set[ColRefBase] = Set()
}

// subquery comparison qualifier
private[scleradb]
sealed abstract class CmpQual
private[scleradb]
case object CmpAll extends CmpQual
private[scleradb]
case object CmpAny extends CmpQual

/** Sort direction */
sealed abstract class SortDir {
    /** How should the NULLs be ordered with respect to non-nulls (values)? */
    def defaultNullsOrder: NullsOrder
}

/** Ascending sort direction */
case object SortAsc extends SortDir {
    override def defaultNullsOrder: NullsOrder = NullsLast
}

/** Descending sort direction */
case object SortDesc extends SortDir {
    override def defaultNullsOrder: NullsOrder = NullsFirst
}

/** Abstract base class for objects that specify the ordering of the NULLs
  * with respect to non-nulls (values)
  */
sealed abstract class NullsOrder
/** Nulls ordered before non-nulls (values) */
case object NullsFirst extends NullsOrder
/** Nulls ordered after non-nulls (values) */
case object NullsLast extends NullsOrder

/** Sort expression
  *
  * @param expr Scalar expression used to determine the ordering
  * @param sortDir Sort direction - ascending or descending
  * @param nullsOrder Ordering of nulls agains non-nulls (values)
  */
case class SortExpr(expr: ScalExpr, sortDir: SortDir, nullsOrder: NullsOrder) {
    /** Is the expression expr an aggregate scalar expression? */
    def isAggregate: Boolean = expr.isAggregate
    /** Columns present within the expression expr */
    def columns: Set[ColRefBase] = expr.columns
    /** Replace this expression, or all occurrences of subexpressions */
    def replace(replMap: Map[ScalExpr, ScalExpr]): SortExpr =
        SortExpr(expr.replace(replMap), sortDir, nullsOrder)
    /** String representation of this sort expression */
    def repr: String = SqlMapper.sortExprString(this)
}

// utility functions
private[scleradb]
object SortExpr {
    def apply(expr: ScalExpr): SortExpr = apply(expr, SortAsc)

    def apply(expr: ScalExpr, dir: SortDir): SortExpr =
        SortExpr(expr, dir, dir.defaultNullsOrder)

    def isSubsumedBy(a: List[SortExpr], b: List[SortExpr]): Boolean =
        ( a.size <= b.size ) && {
            a.zip(b).forall { // prefix match
                case (aSortExpr, bSortExpr) => (aSortExpr == bSortExpr)
            }
        }

    def compatiblePartnCols(
        sortExprs: List[SortExpr],
        partnCols: List[ColRef]
    ): List[ColRef] = {
        val partnSortExprs: List[SortExpr] =
            sortExprs.takeWhile {
                case SortExpr(col: ColRef, _, _) => partnCols contains col
                case _ => false
            }

        partnSortExprs.flatMap {
            case SortExpr(col: ColRef, _, _) => Some(col)
            case _ => None
        }
    }
}

// targeted expression for project
private[scleradb]
sealed abstract class TargetExpr {
    def isAggregate: Boolean
    def isNestedAggregate: Boolean
    def repr: String = SqlMapper.targetExprString(this)
}

private[scleradb]
case class StarTargetExpr(
    tableNameOpt: Option[(String, Option[Int])],
    exceptColRefs: List[ColRef] = Nil
) extends TargetExpr {
    override def isAggregate: Boolean = false
    override def isNestedAggregate: Boolean = false
}

private[scleradb]
sealed abstract class ScalarTarget extends TargetExpr {
    val expr: ScalExpr
    val alias: ColRef

    def columns: Set[ColRefBase] = expr.columns

    def replace(replMap: Map[ScalExpr, ScalExpr]): ScalarTarget =
        ScalarTarget(expr.replace(replMap), alias)

    override def isAggregate: Boolean = expr.isAggregate
    override def isNestedAggregate: Boolean = expr.isNestedAggregate
}

private[scleradb]
object ScalarTarget {
    def apply(expr: ScalExpr, alias: ColRef): ScalarTarget = expr match {
        case (value: ScalColValue) => ValueCol(value, alias)
        case (col: ColRef) => RenameCol(col, alias)
        case other => AliasedExpr(other, alias)
    }
}

private[scleradb]
case class AliasedExpr(
    override val expr: ScalExpr,
    override val alias: ColRef
) extends ScalarTarget

private[scleradb]
sealed abstract class RenameTarget extends ScalarTarget {
    override val expr: ScalExpr
    override val alias: ColRef
}

private[scleradb]
case class RenameCol(
    override val expr: ColRef,
    override val alias: ColRef
) extends RenameTarget

private[scleradb]
case class ValueCol(
    override val expr: ScalColValue,
    override val alias: ColRef
) extends RenameTarget
