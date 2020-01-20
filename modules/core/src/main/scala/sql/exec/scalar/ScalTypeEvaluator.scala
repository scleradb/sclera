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

import com.scleradb.sql.types._
import com.scleradb.sql.expr._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.ScalTableRow

private[scleradb]
object ScalTypeEvaluator {
    // evalue the type of a constant expression
    def eval(
        scalExpr: ScalExpr
    ): SqlType = eval(scalExpr, ScalTableRow(Nil))

    // evalue the type of an expression given the schema
    def eval(
        scalExpr: ScalExpr,
        columns: List[Column]
    ): SqlType = scalExpr match {
        case (value: ScalValueBase) => value.sqlBaseType
        case (sqlNull: SqlNull) => sqlNull.sqlBaseType
        case _ =>
            val nullRow: ScalTableRow = ScalTableRow(
                columns.map { col =>
                    (col.name -> SqlNull(col.sqlType.baseType))
                }
            )

            eval(scalExpr, nullRow)
    }

    def eval(
        scalExpr: ScalExpr,
        row: ScalTableRow
    ): SqlType = scalExpr match {
        case (value: ScalValueBase) => value.sqlBaseType
        case (sqlNull: SqlNull) => sqlNull.sqlBaseType
        case (colRef: ColRef) => row.getScalExpr(colRef).sqlBaseType

        case CaseExpr(_, _, defaultExpr) => eval(defaultExpr, row)

        case Exists(_) => SqlBool

        case ScalOpExpr(op, inputs) =>
            eval(op, inputs.map { input => eval(input, row).baseType })

        case _ => SqlNull().sqlBaseType
    }

    // evaluate operator expression on constant or null inputs
    private def eval(
        op: ScalOp,
        inputs: List[SqlType]
    ): SqlType = (op, inputs) match {
        case (TypeCast(t), _) => t.baseType

        case (IsNull, _) => SqlBool
        case (IsDistinctFrom, _) => SqlBool

        case (UnaryPlus, List(t)) => t

        case (UnaryMinus, List(SqlSmallInt)) => SqlInteger
        case (UnaryMinus, List(SqlInteger)) => SqlInteger
        case (UnaryMinus, List(SqlBigInt)) => SqlBigInt
        case (UnaryMinus, List(t)) => t

        case (Plus, List(_: SqlDoublePrecFloatingPoint, _)) => SqlFloat(None)
        case (Plus, List(_, _: SqlDoublePrecFloatingPoint)) => SqlFloat(None)
        case (Plus, List(_: SqlSinglePrecFloatingPoint, _)) => SqlReal
        case (Plus, List(_, _: SqlSinglePrecFloatingPoint)) => SqlReal
        case (Plus, List(SqlSmallInt, SqlSmallInt)) => SqlInteger
        case (Plus, List(_, _)) => SqlBigInt

        case (Minus, List(lhsType, rhsType)) =>
            eval(Plus, List(lhsType, eval(UnaryMinus, List(rhsType))))

        case (Mult, List(_: SqlDoublePrecFloatingPoint, _)) => SqlFloat(None)
        case (Mult, List(_, _: SqlDoublePrecFloatingPoint)) => SqlFloat(None)
        case (Mult, List(_: SqlSinglePrecFloatingPoint, _)) => SqlFloat(None)
        case (Mult, List(_, _: SqlSinglePrecFloatingPoint)) => SqlFloat(None)
        case (Mult, List(_, _)) => SqlBigInt

        case (Div, List(_: SqlDoublePrecFloatingPoint, _)) => SqlFloat(None)
        case (Div, List(_, _: SqlDoublePrecFloatingPoint)) => SqlFloat(None)
        case (Div, List(_: SqlSinglePrecFloatingPoint, _)) => SqlFloat(None)
        case (Div, List(_, _: SqlSinglePrecFloatingPoint)) => SqlFloat(None)
        case (Div, List(t, _)) => t

        case (Modulo, List(_: SqlDoublePrecFloatingPoint, _)) => SqlFloat(None)
        case (Modulo, List(_, _: SqlDoublePrecFloatingPoint)) => SqlFloat(None)
        case (Modulo, List(_: SqlSinglePrecFloatingPoint, _)) => SqlReal
        case (Modulo, List(_, _: SqlSinglePrecFloatingPoint)) => SqlReal
        case (Modulo, List(_, t)) => t

        case (Exp, List(_, _)) => SqlFloat(None)

        case (Not, _) => SqlBool
        case (Or, _) => SqlBool
        case (And, _) => SqlBool
        case (IsLike(_), _) => SqlBool
        case (IsILike(_), _) => SqlBool
        case (IsSimilarTo(_), _) => SqlBool
        case (_: ScalRelCmpOp, _) => SqlBool
        case (IsBetween(_), _) => SqlBool

        case (ScalarFunction(name), ts) =>
            ScalFunctionEvaluator.scalarFunctions.get(name) match {
                case Some(f) => f.resultType(ts)
                case None =>
                    throw new IllegalArgumentException(
                        "Unable to evaluate unknown function \"" + name + "\""
                    )
            }

        case _ => SqlNull().sqlBaseType
    }
}
