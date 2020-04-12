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
sealed abstract class ScalOp {
    def isAggregate: Boolean
    def defaultAlias(inputs: List[ScalExpr]): String = "EXPR"
}

// type cast
case class TypeCast(t: SqlType) extends ScalOp {
    override def isAggregate: Boolean = false
    override def defaultAlias(inputs: List[ScalExpr]): String = t.repr
}

// unary prefix expression operators
trait UnaryPrefix extends ScalOp

// unary postfix expression operators
trait UnaryPostfix extends ScalOp

// binary infix expression operators
trait BinaryInfix extends ScalOp

// arithmetic expression operators
sealed abstract class ArithOp extends ScalOp {
    override def isAggregate: Boolean = false
}

// unary arithmetic expression operators
sealed abstract class UnaryPrefixArithOp extends ArithOp with UnaryPrefix
case object UnaryPlus extends UnaryPrefixArithOp
case object UnaryMinus extends UnaryPrefixArithOp

// binary arithmetic expression operators
sealed abstract class BinaryInfixArithOp extends ArithOp with BinaryInfix
case object Plus extends BinaryInfixArithOp
case object Minus extends BinaryInfixArithOp
case object Mult extends BinaryInfixArithOp
case object Div extends BinaryInfixArithOp
case object Modulo extends BinaryInfixArithOp
case object Exp extends BinaryInfixArithOp

// boolean expression (predicate) operators
sealed abstract class BooleanOp extends ScalOp {
    override def isAggregate: Boolean = false
}

// unary boolean expression operators
sealed abstract class UnaryPrefixBooleanOp extends BooleanOp with UnaryPrefix
case object Not extends UnaryPrefixBooleanOp

// binary boolean expression operators
sealed abstract class BinaryInfixBooleanOp extends BooleanOp with BinaryInfix
case object Or extends BinaryInfixBooleanOp
case object And extends BinaryInfixBooleanOp

// comparison operators
sealed abstract class ScalCmpOp extends ScalOp {
    override def isAggregate: Boolean = false
}

// unary postfix comparison operators
sealed abstract class UnaryPostfixScalCmpOp extends ScalCmpOp with UnaryPostfix
case object IsNull extends UnaryPostfixScalCmpOp

// pattern matching
sealed abstract class PatternMatchOp extends UnaryPostfixScalCmpOp
case class IsLike(pattern: Pattern) extends PatternMatchOp
case class IsILike(pattern: Pattern) extends PatternMatchOp
case class IsSimilarTo(pattern: Pattern) extends PatternMatchOp

// binary infix comparison operators
sealed abstract class BinaryInfixScalCmpOp extends ScalCmpOp with BinaryInfix

case object IsDistinctFrom extends BinaryInfixScalCmpOp

case class IsBetween(qual: RangeQual) extends BinaryInfixScalCmpOp

// range qualifier
sealed abstract class RangeQual
case object Symmetric extends RangeQual
case object Asymmetric extends RangeQual

// comparison operators on scalars and relational/scalar expressions
sealed abstract class ScalRelCmpOp extends ScalOp with BinaryInfix {
    override def isAggregate: Boolean = false
}

case object Equals extends ScalRelCmpOp
case object NotEquals extends ScalRelCmpOp
case object LessThan extends ScalRelCmpOp
case object LessThanEq extends ScalRelCmpOp
case object GreaterThan extends ScalRelCmpOp
case object GreaterThanEq extends ScalRelCmpOp

// functions
abstract class Function extends ScalOp {
    val name: String
    override def defaultAlias(inputs: List[ScalExpr]): String = name
}

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
case class AggregateFunction(
    override val name: String,
    qual: FuncQual
) extends Function {
    override def isAggregate: Boolean = true
}

// scalar function
case class ScalarFunction(override val name: String) extends Function {
    override def isAggregate: Boolean = false
}

// scalar function
case class LabeledFunction(
    override val name: String,
    labels: List[String]
) extends Function {
    override def isAggregate: Boolean = false
}

// function qualifier
sealed abstract class FuncQual
case object FuncAll extends FuncQual
case object FuncDistinct extends FuncQual
