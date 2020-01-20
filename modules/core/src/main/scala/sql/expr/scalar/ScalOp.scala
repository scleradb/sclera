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

import com.scleradb.util.tools.Counter
import com.scleradb.sql.types.SqlType
import com.scleradb.sql.exec.ScalFunctionEvaluator

// scalar expression operators
private[scleradb]
sealed abstract class ScalOp {
    def isAggregate: Boolean
    def defaultAlias(inputs: List[ScalExpr]): String = "EXPR"
}

// type cast
private[scleradb]
case class TypeCast(t: SqlType) extends ScalOp {
    override def isAggregate: Boolean = false
    override def defaultAlias(inputs: List[ScalExpr]): String = t.repr
}

// unary prefix expression operators
private[scleradb]
trait UnaryPrefix extends ScalOp

// unary postfix expression operators
private[scleradb]
trait UnaryPostfix extends ScalOp

// binary infix expression operators
private[scleradb]
trait BinaryInfix extends ScalOp

// arithmetic expression operators
private[scleradb]
sealed abstract class ArithOp extends ScalOp {
    override def isAggregate: Boolean = false
}

// unary arithmetic expression operators
private[scleradb]
sealed abstract class UnaryPrefixArithOp extends ArithOp with UnaryPrefix
private[scleradb]
case object UnaryPlus extends UnaryPrefixArithOp
private[scleradb]
case object UnaryMinus extends UnaryPrefixArithOp

// binary arithmetic expression operators
private[scleradb]
sealed abstract class BinaryInfixArithOp extends ArithOp with BinaryInfix
private[scleradb]
case object Plus extends BinaryInfixArithOp
private[scleradb]
case object Minus extends BinaryInfixArithOp
private[scleradb]
case object Mult extends BinaryInfixArithOp
private[scleradb]
case object Div extends BinaryInfixArithOp
private[scleradb]
case object Modulo extends BinaryInfixArithOp
private[scleradb]
case object Exp extends BinaryInfixArithOp

// boolean expression (predicate) operators
private[scleradb]
sealed abstract class BooleanOp extends ScalOp {
    override def isAggregate: Boolean = false
}

// unary boolean expression operators
private[scleradb]
sealed abstract class UnaryPrefixBooleanOp extends BooleanOp with UnaryPrefix
private[scleradb]
case object Not extends UnaryPrefixBooleanOp

// binary boolean expression operators
private[scleradb]
sealed abstract class BinaryInfixBooleanOp extends BooleanOp with BinaryInfix
private[scleradb]
case object Or extends BinaryInfixBooleanOp
private[scleradb]
case object And extends BinaryInfixBooleanOp

// comparison operators
private[scleradb]
sealed abstract class ScalCmpOp extends ScalOp {
    override def isAggregate: Boolean = false
}

// unary postfix comparison operators
private[scleradb]
sealed abstract class UnaryPostfixScalCmpOp extends ScalCmpOp with UnaryPostfix
private[scleradb]
case object IsNull extends UnaryPostfixScalCmpOp

// pattern matching
private[scleradb]
sealed abstract class PatternMatchOp extends UnaryPostfixScalCmpOp
private[scleradb]
case class IsLike(pattern: Pattern) extends PatternMatchOp
private[scleradb]
case class IsILike(pattern: Pattern) extends PatternMatchOp
private[scleradb]
case class IsSimilarTo(pattern: Pattern) extends PatternMatchOp

// binary infix comparison operators
private[scleradb]
sealed abstract class BinaryInfixScalCmpOp extends ScalCmpOp with BinaryInfix

private[scleradb]
case object IsDistinctFrom extends BinaryInfixScalCmpOp

private[scleradb]
case class IsBetween(qual: RangeQual) extends BinaryInfixScalCmpOp

// range qualifier
private[scleradb]
sealed abstract class RangeQual
private[scleradb]
case object Symmetric extends RangeQual
private[scleradb]
case object Asymmetric extends RangeQual

// comparison operators on scalars and relational/scalar expressions
private[scleradb]
sealed abstract class ScalRelCmpOp extends ScalOp with BinaryInfix {
    override def isAggregate: Boolean = false
}

private[scleradb]
case object Equals extends ScalRelCmpOp
private[scleradb]
case object NotEquals extends ScalRelCmpOp
private[scleradb]
case object LessThan extends ScalRelCmpOp
private[scleradb]
case object LessThanEq extends ScalRelCmpOp
private[scleradb]
case object GreaterThan extends ScalRelCmpOp
private[scleradb]
case object GreaterThanEq extends ScalRelCmpOp

// functions
private[scleradb]
abstract class Function extends ScalOp {
    val name: String
    override def defaultAlias(inputs: List[ScalExpr]): String = name
}

private[scleradb]
object Function {
    def apply(name: String, qualOpt: Option[FuncQual]) =
        if( ScalFunctionEvaluator.aggregateFunctions.contains(name) )
            AggregateFunction(name, qualOpt.getOrElse(FuncAll))
        else if( qualOpt.isEmpty )
            ScalarFunction(name)
        else throw new IllegalArgumentException(
            "Invalid argument qualifier for non-aggregate function \"" +
             name + "\""
        )
}

// function (scalar or aggregate)
private[scleradb]
case class AggregateFunction(
    override val name: String,
    qual: FuncQual
) extends Function {
    override def isAggregate: Boolean = true
}

// scalar function
private[scleradb]
case class ScalarFunction(override val name: String) extends Function {
    override def isAggregate: Boolean = false
}

// scalar function
private[scleradb]
case class LabeledFunction(
    override val name: String,
    labels: List[String]
) extends Function {
    override def isAggregate: Boolean = false
}

// function qualifier
private[scleradb]
sealed abstract class FuncQual
private[scleradb]
case object FuncAll extends FuncQual
private[scleradb]
case object FuncDistinct extends FuncQual
