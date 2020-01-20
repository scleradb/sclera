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

import com.scleradb.exec.Processor

import com.scleradb.sql.statements.SqlRelQueryStatement
import com.scleradb.sql.types._
import com.scleradb.sql.expr._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.ScalTableRow

import com.scleradb.external.expr.ExternalScalarFunction

private[scleradb]
class ScalExprEvaluator(processor: Processor) {
    import com.scleradb.sql.exec.ScalCastEvaluator._

    // evalue a constant expression
    def eval(
        scalExpr: ScalExpr
    ): ScalColValue = eval(scalExpr, ScalTableRow(Nil))

    def eval(
        scalExpr: ScalExpr,
        row: ScalTableRow
    ): ScalColValue = scalExpr match {
        case (value: ScalValueBase) => value

        case (sqlNull: SqlNull) => sqlNull

        case (colRef: ColRef) => row.getScalExpr(colRef)

        case caseExpr@CaseExpr(argExpr, whenThen, defaultExpr) =>
            eval(argExpr, row) match {
                case (sqlNull: SqlNull) =>
                    SqlNull(ScalTypeEvaluator.eval(caseExpr, row))
                case argVal =>
                    val matchedEntryOpt: Option[(ScalExpr, ScalExpr)] =
                        whenThen.find { case (whenExpr, _) =>
                            eval(whenExpr, row) match {
                                case (_: SqlNull) => false
                                case (whenVal: ScalValueBase) =>
                                    whenVal == argVal
                            }
                        }

                    val matchedExpr: ScalExpr = matchedEntryOpt match {
                        case Some((_, thenExpr)) => thenExpr
                        case None => defaultExpr
                    }

                    eval(matchedExpr, row)
            }

        case ScalSubQuery(relExpr) =>
            processor.handleQueryStatement(
                SqlRelQueryStatement(relExpr), { rs =>
                    val col: Column = rs.columns.head
                    rs.typedRows.take(1).toList.headOption match {
                        case Some(subQueryRow) => subQueryRow.getScalExpr(col)
                        case None => SqlNull(col.sqlType)
                    }
                }
            )

        case Exists(relExpr: RelExpr) =>
            processor.handleQueryStatement(
                SqlRelQueryStatement(relExpr),
                { rs => BoolConst(rs.rows.hasNext) }
            )

        case ScalOpExpr(cmpOp: ScalRelCmpOp,
                        List(lhs, ScalCmpRelExpr(qual, rhs))) =>
            eval(lhs, row) match {
                case (_: SqlNull) => SqlNull(SqlBool)
                case (lhsVal: ScalValueBase) =>
                    val resultOpt: Option[Boolean] = rhs match {
                        case ScalarList(scalars) =>
                            compareOpt(cmpOp, qual, lhsVal, scalars.iterator)

                        case RelSubQuery(relExpr) =>
                            processor.handleQueryStatement(
                                SqlRelQueryStatement(relExpr), { rs =>
                                    val col: Column = rs.columns.head

                                    // iterator available only within this block
                                    val scalars: Iterator[ScalColValue] =
                                        rs.rows.map { t => t.getScalExpr(col) }

                                    compareOpt(cmpOp, qual, lhsVal, scalars)
                                }
                            )
                    }

                    resultOpt match {
                        case Some(result) => BoolConst(result)
                        case None => SqlNull(SqlBool)
                    }
            }

        case ScalOpExpr(op, inputs) =>
            eval(op, inputs.map { input => eval(input, row) })

        case reject =>
            throw new IllegalArgumentException(
                "Unable to evaluate expression: " + reject.repr
            )
    }

    // evaluate operator expression on constant or null inputs
    private def eval(
        op: ScalOp,
        inputs: List[ScalColValue]
    ): ScalColValue = (op, inputs) match {
        case (TypeCast(t), List(v)) => castScalColValue(v, t)

        case (IsNull, List(_: SqlNull)) => BoolConst(true)
        case (IsNull, List(_: ScalValueBase)) => BoolConst(false)

        case (IsDistinctFrom, List(lhs, rhs)) => BoolConst(lhs != rhs)

        case (UnaryPlus, List(sqlNull: SqlNull)) => sqlNull
        case (UnaryPlus, List(input: NumericConst)) => input

        case (UnaryMinus, List(sqlNull: SqlNull)) => sqlNull
        case (UnaryMinus, List(FloatConst(v)))
        if v.isNaN || v.isInfinite => FloatConst(-v)
        case (UnaryMinus, List(DoubleConst(v)))
        if v.isNaN || v.isInfinite => DoubleConst(-v)

        case (UnaryMinus, List(ShortConst(v))) => IntConst(toInt(-toInt(v)))
        case (UnaryMinus, List(IntConst(v))) => IntConst(toInt(-toLong(v)))
        case (UnaryMinus, List(LongConst(v))) =>
            LongConst(toLong(-toBigDecimal(v)))
        case (UnaryMinus, List(FloatConst(v))) =>
            FloatConst(toFloat(-toDouble(v)))
        case (UnaryMinus, List(DoubleConst(v))) =>
            DoubleConst(toDouble(-toBigDecimal(v)))

        case (Plus, List(sqlNull: SqlNull, _)) => sqlNull
        case (Plus, List(v, _: SqlNull)) => SqlNull(v.sqlBaseType)
        case (Plus, List(x@FloatConst(v), _))
        if v.isNaN || v.isInfinite => x
        case (Plus, List(x@DoubleConst(v), _))
        if v.isNaN || v.isInfinite => x
        case (Plus, List(_, x@FloatConst(v)))
        if v.isNaN || v.isInfinite => x
        case (Plus, List(_, x@DoubleConst(v)))
        if v.isNaN || v.isInfinite => x

        case (Plus, List(ShortConst(lhs), ShortConst(rhs))) =>
            IntConst(toInt(toLong(lhs) + toLong(rhs)))
        case (Plus, List(IntConst(lhs), ShortConst(rhs))) =>
            LongConst(toLong(toLong(lhs) + toLong(rhs)))
        case (Plus, List(IntConst(lhs), IntConst(rhs))) =>
            LongConst(toLong(toLong(lhs) + toLong(rhs)))
        case (Plus, List(LongConst(lhs), ShortConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) + toBigDecimal(rhs)))
        case (Plus, List(LongConst(lhs), IntConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) + toBigDecimal(rhs)))
        case (Plus, List(LongConst(lhs), LongConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) + toBigDecimal(rhs)))
        case (Plus, List(FloatConst(lhs), ShortConst(rhs))) =>
            DoubleConst(toDouble(toDouble(lhs) + toDouble(rhs)))
        case (Plus, List(FloatConst(lhs), IntConst(rhs))) =>
            DoubleConst(toDouble(toDouble(lhs) + toDouble(rhs)))
        case (Plus, List(FloatConst(lhs), LongConst(rhs))) =>
            DoubleConst(toDouble(toDouble(lhs) + toDouble(rhs)))
        case (Plus, List(FloatConst(lhs), FloatConst(rhs))) =>
            DoubleConst(toDouble(toDouble(lhs) + toDouble(rhs)))
        case (Plus, List(DoubleConst(lhs), ShortConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) + rhs))
        case (Plus, List(DoubleConst(lhs), IntConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) + toBigDecimal(rhs)))
        case (Plus, List(DoubleConst(lhs), LongConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) + toBigDecimal(rhs)))
        case (Plus, List(DoubleConst(lhs), FloatConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) + toBigDecimal(rhs)))
        case (Plus, List(DoubleConst(lhs), DoubleConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) + toBigDecimal(rhs)))
        case (Plus, List(lhsConst: NumericConst, rhsConst: NumericConst)) =>
            eval(Plus, List(rhsConst, lhsConst))

        case (Plus, List(v: DateTimeConst, _: SqlNull)) =>
            SqlNull(v.sqlBaseType)
        case (Plus, List(lhsConst: DateTimeConst, rhsConst: IntegralConst)) =>
            lhsConst.add(rhsConst.integralValue)

        case (Minus, List(sqlNull: SqlNull, _: NumericConst)) => sqlNull
        case (Minus, List(v: NumericConst, _: SqlNull)) =>
            SqlNull(v.sqlBaseType)
        case (Minus, List(lhsConst: NumericConst, rhsConst: NumericConst)) =>
            eval(Plus, List(lhsConst, eval(UnaryMinus, List(rhsConst))))

        case (Minus, List(sqlNull: SqlNull, _: SqlNull)) => sqlNull
        case (Minus, List(_: SqlNull, _: DateTimeConst)) => SqlNull(SqlBigInt)
        case (Minus, List(v: DateTimeConst,
                          SqlTypedNull(SqlDate | SqlTime | SqlTimestamp))) =>
            SqlNull(SqlBigInt)
        case (Minus, List(v: DateTimeConst, _: SqlNull)) =>
            SqlNull(v.sqlBaseType)
        case (Minus, List(lhsConst: DateTimeConst, rhsConst: DateTimeConst)) =>
            lhsConst.subtract(rhsConst)
        case (Minus, List(lhsConst: DateTimeConst, rhsConst: IntegralConst)) =>
            lhsConst.add(-rhsConst.integralValue)

        case (Mult, List(sqlNull: SqlNull, _)) => sqlNull
        case (Mult, List(v, _: SqlNull)) => SqlNull(v.sqlBaseType)
        case (Mult, List(FloatConst(v), x: NumericConst))
        if v.isNaN || v.isInfinite => DoubleConst(v * x.numericValue)
        case (Mult, List(DoubleConst(v), x: NumericConst))
        if v.isNaN || v.isInfinite => DoubleConst(v * x.numericValue)
        case (Mult, List(x: NumericConst, FloatConst(v)))
        if v.isNaN || v.isInfinite => DoubleConst(x.numericValue * v)
        case (Mult, List(x: NumericConst, DoubleConst(v)))
        if v.isNaN || v.isInfinite => DoubleConst(x.numericValue * v)

        case (Mult, List(ShortConst(lhs), ShortConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(IntConst(lhs), ShortConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(IntConst(lhs), IntConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(LongConst(lhs), ShortConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(LongConst(lhs), IntConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(LongConst(lhs), LongConst(rhs))) =>
            LongConst(toLong(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(FloatConst(lhs), ShortConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(FloatConst(lhs), IntConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(FloatConst(lhs), LongConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(FloatConst(lhs), FloatConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(DoubleConst(lhs), ShortConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(DoubleConst(lhs), IntConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(DoubleConst(lhs), LongConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(DoubleConst(lhs), FloatConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(DoubleConst(lhs), DoubleConst(rhs))) =>
            DoubleConst(toDouble(toBigDecimal(lhs) * toBigDecimal(rhs)))
        case (Mult, List(lhsConst: NumericConst, rhsConst: NumericConst)) =>
            eval(Mult, List(rhsConst, lhsConst))

        case (Div, List(sqlNull: SqlNull, _)) => sqlNull
        case (Div, List(v, _: SqlNull)) => SqlNull(v.sqlBaseType)
        case (Div, List(FloatConst(v), x: NumericConst))
        if v.isNaN || v.isInfinite => DoubleConst(v / x.numericValue)
        case (Div, List(DoubleConst(v), x: NumericConst))
        if v.isNaN || v.isInfinite => DoubleConst(v / x.numericValue)
        case (Div, List(x: NumericConst, FloatConst(v)))
        if v.isNaN || v.isInfinite => DoubleConst(x.numericValue / v)
        case (Div, List(x: NumericConst, DoubleConst(v)))
        if v.isNaN || v.isInfinite => DoubleConst(x.numericValue / v)

        case (Div, List(lhsConst: IntegralConst, FloatConst(rhs))) =>
            DoubleConst(toDouble(toDouble(lhsConst.integralValue) / rhs))
        case (Div, List(lhsConst: IntegralConst, DoubleConst(rhs))) =>
            DoubleConst(toDouble(toDouble(lhsConst.integralValue) / rhs))
        case (Div, List(ShortConst(lhs), rhsConst: IntegralConst)) =>
            ShortConst(toShort(toLong(lhs) / rhsConst.integralValue))
        case (Div, List(IntConst(lhs), rhsConst: IntegralConst)) =>
            IntConst(toInt(toLong(lhs) / rhsConst.integralValue))
        case (Div, List(LongConst(lhs), rhsConst: IntegralConst)) =>
            LongConst(toLong(toBigDecimal(lhs) / rhsConst.integralValue))
        case (Div, List(FloatConst(lhs), rhsConst: NumericConst)) =>
            DoubleConst(toDouble(toBigDecimal(lhs) / rhsConst.numericValue))
        case (Div, List(DoubleConst(lhs), rhsConst: NumericConst)) =>
            DoubleConst(toDouble(toBigDecimal(lhs) / rhsConst.numericValue))

        case (Modulo, List(sqlNull: SqlNull, _)) => sqlNull
        case (Modulo, List(v, _: SqlNull)) => SqlNull(v.sqlBaseType)
        case (Modulo, List(FloatConst(v), x: NumericConst))
        if v.isNaN || v.isInfinite => DoubleConst(v % x.numericValue)
        case (Modulo, List(DoubleConst(v), x: NumericConst))
        if v.isNaN || v.isInfinite => DoubleConst(v % x.numericValue)
        case (Modulo, List(x: NumericConst, FloatConst(v)))
        if v.isNaN || v.isInfinite => DoubleConst(x.numericValue % v)
        case (Modulo, List(x: NumericConst, DoubleConst(v)))
        if v.isNaN || v.isInfinite => DoubleConst(x.numericValue % v)

        case (Modulo, List(lhsConst: IntegralConst, FloatConst(rhs))) =>
            FloatConst(toFloat(lhsConst.integralValue % rhs))
        case (Modulo, List(lhsConst: IntegralConst, DoubleConst(rhs))) =>
            DoubleConst(toDouble(lhsConst.integralValue % rhs))
        case (Modulo, List(ShortConst(lhs), rhsConst: IntegralConst)) =>
            ShortConst(toShort(lhs % rhsConst.integralValue))
        case (Modulo, List(lhsConst: IntegralConst, ShortConst(rhs))) =>
            ShortConst(toShort(lhsConst.integralValue % rhs))
        case (Modulo, List(IntConst(lhs), rhsConst: IntegralConst)) =>
            IntConst(toInt(lhs % rhsConst.integralValue))
        case (Modulo, List(lhsConst: IntegralConst, IntConst(rhs))) =>
            IntConst(toInt(lhsConst.integralValue % rhs))
        case (Modulo, List(LongConst(lhs), LongConst(rhs))) =>
            LongConst(lhs % rhs)
        case (Modulo, List(FloatConst(lhs), rhsConst: NumericConst)) =>
            DoubleConst(toDouble(lhs % rhsConst.numericValue))
        case (Modulo, List(DoubleConst(lhs), rhsConst: NumericConst)) =>
            DoubleConst(toDouble(lhs % rhsConst.numericValue))

        case (Exp, List(sqlNull: SqlNull, v)) => sqlNull
        case (Exp, List(v, sqlNull: SqlNull)) => SqlNull(v.sqlBaseType)
        case (Exp, List(lhsConst: NumericConst, rhsConst: NumericConst)) =>
            DoubleConst(
                scala.math.pow(lhsConst.numericValue, rhsConst.numericValue)
            )

        case (Not, List(_: SqlNull)) => SqlNull(SqlBool)
        case (Not, List(BoolConst(v))) => BoolConst(!v)

        case (Or, List(_, res@BoolConst(true))) => res
        case (Or, List(res@BoolConst(true), _)) => res
        case (Or, List(_: SqlNull, _)) => SqlNull(SqlBool)
        case (Or, List(_, _: SqlNull)) => SqlNull(SqlBool)
        case (Or, List(_, _)) => BoolConst(false)

        case (And, List(_, res@BoolConst(false))) => res
        case (And, List(res@BoolConst(false), _)) => res
        case (And, List(_: SqlNull, _)) => SqlNull(SqlBool)
        case (And, List(_, _: SqlNull)) => SqlNull(SqlBool)
        case (And, List(_, _)) => BoolConst(true)

        case (IsLike(_), List(_: SqlNull)) => SqlNull(SqlBool)
        case (IsLike(Pattern(pat, esc)), lhs) =>
            val escLiteral: String = if( esc == "\\") (esc + esc) else esc
            val rewrite: String =
                pat.replaceAllLiterally(esc + "_", "$SCLERA1$").
                    replaceAllLiterally(esc + "%", "$SCLERA2$").
                    replaceAllLiterally(esc + esc, "$SCLERA3$").
                    replaceAllLiterally(".", "\\.").
                    replaceAllLiterally("*", "\\*").
                    replaceAllLiterally("_", ".").
                    replaceAllLiterally("%", ".*").
                    replaceAllLiterally("$SCLERA1$", "_").
                    replaceAllLiterally("$SCLERA2$", "%").
                    replaceAllLiterally("$SCLERA3$", escLiteral)

            eval(IsSimilarTo(Pattern(rewrite, "\\")), lhs)

        case (IsILike(_), List(_: SqlNull)) => SqlNull(SqlBool)
        case (IsILike(Pattern(pat, esc)), List(CharConst(s))) =>
            eval(
                IsLike(Pattern(pat.toUpperCase, esc.toUpperCase)),
                List(CharConst(s.toUpperCase))
            )

        case (IsSimilarTo(_), List(_: SqlNull)) => SqlNull(SqlBool)
        case (IsSimilarTo(Pattern(pat, esc)), List(CharConst(s))) =>
            val rewrite: String =
                if( esc == "\\" ) pat
                else pat.replaceAllLiterally(esc, "$SCLERAESC$").
                         replaceAllLiterally("\\", "\\\\").
                         replaceAllLiterally("$SCLERAESC$", "\\")

            BoolConst(s.matches(rewrite))

        case (_: ScalRelCmpOp, List(_: SqlNull, _)) => SqlNull(SqlBool)
        case (_: ScalRelCmpOp, List(_, _: SqlNull)) => SqlNull(SqlBool)
        case (cmpOp: ScalRelCmpOp,
              List(lhsVal: ScalValueBase, rhsVal: ScalValueBase)) =>
            BoolConst(compare(cmpOp, lhsVal, rhsVal))

        case (IsBetween(_), List(_: SqlNull, _, _)) => SqlNull(SqlBool)
        case (IsBetween(_), List(_, _: SqlNull, _)) => SqlNull(SqlBool)
        case (IsBetween(_), List(_, _, _: SqlNull)) => SqlNull(SqlBool)
        case (IsBetween(Symmetric),
              List(v: ScalValueBase, lhs: ScalValueBase, rhs: ScalValueBase)) =>
            if( compare(LessThanEq, lhs, rhs) )
                BoolConst(compare(LessThanEq, lhs, v) &&
                          compare(LessThanEq, v, rhs))
            else BoolConst(compare(LessThanEq, rhs, v) &&
                           compare(LessThanEq, v, lhs))
        case (IsBetween(Asymmetric),
              List(v: ScalValueBase, lhs: ScalValueBase, rhs: ScalValueBase)) =>
            BoolConst(compare(LessThanEq, lhs, v) &&
                      compare(LessThanEq, v, rhs))

        case (ScalarFunction(name), params) =>
            ScalFunctionEvaluator.scalarFunctions.get(name) match {
                case Some(f) => f.eval(params)
                case None =>
                    throw new IllegalArgumentException(
                        "Unable to evaluate unknown function \"" + name + "\""
                    )
            }

        case (ExternalScalarFunction(extFunction), params) =>
            extFunction.result(params)

        case (rejectOp, rejectParams) =>
            throw new IllegalArgumentException(
                "Unable to evaluate expression: " +
                ScalOpExpr(rejectOp, rejectParams).repr
            )
    }

    private def compare(
        cmpOp: ScalRelCmpOp,
        lhsVal: ScalValueBase,
        rhsVal: ScalValueBase
    ): Boolean = cmpOp match {
        case Equals => (lhsVal compare rhsVal) == 0
        case NotEquals => (lhsVal compare rhsVal) != 0
        case LessThan => (lhsVal compare rhsVal) < 0
        case LessThanEq => (lhsVal compare rhsVal) <= 0
        case GreaterThan => (lhsVal compare rhsVal) > 0
        case GreaterThanEq => (lhsVal compare rhsVal) >= 0
    }

    private def compareOpt(
        cmpOp: ScalRelCmpOp,
        qual: CmpQual,
        lhsVal: ScalValueBase,
        rhsValOptIter: Iterator[ScalColValue]
    ): Option[Boolean] = qual match {
        case CmpAll =>
            // null present -> null
            // else all match -> true
            // else -> false
            val remOptIter: Iterator[ScalColValue] =
                rhsValOptIter.dropWhile {
                    case (_: SqlNull) => false
                    case (rhsVal: ScalValueBase) =>
                        compare(cmpOp, lhsVal, rhsVal)
                }

            if( remOptIter.hasNext ) {
                // match failed
                remOptIter.next match {
                    case (_: SqlNull) => None // found a null
                    case _ => Some(false) // found a mismatch
                }
            } else Some(true) // all matched

        case CmpAny =>
            // match exists -> true
            // else null present -> null
            // else -> false
            val remOptIter: Iterator[ScalColValue] =
                rhsValOptIter.dropWhile {
                    case (_: SqlNull) => false
                    case (rhsVal: ScalValueBase) =>
                        !compare(cmpOp, lhsVal, rhsVal)
                }

            if( remOptIter.hasNext ) {
                remOptIter.next match {
                    case (_: SqlNull) => // found a null
                        val matchExists: Boolean =
                            remOptIter.exists {
                                case (_: SqlNull) => false // ignore
                                case (rhsVal: ScalValueBase) =>
                                    compare(cmpOp, lhsVal, rhsVal)
                            }

                        if( matchExists ) Some(true) else None
                    case _ => Some(true) // found a match
                }
            } else Some(false) // none matched
    }
}
