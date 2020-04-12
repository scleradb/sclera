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

package com.scleradb.analytics.transform.expr

import com.scleradb.sql.expr.{RelExpr, RelOpExpr, Order, ExtendedRelOp}
import com.scleradb.sql.expr.{ScalExpr, ColRef, AnnotColRef, SortExpr}

import com.scleradb.analytics.transform.objects.Transformer

sealed abstract class Transform extends ExtendedRelOp {
    val transformType: Transform.TransformType

    val transformer: Transformer

    // logical names -> associated input expression
    val in: List[(String, ScalExpr)]
    // partition input expression
    val partn: List[ScalExpr]
    // partitions of input expression expected sorted on these expressions
    val order: List[SortExpr]
    // logical names -> associated output column
    val out: List[(String, ColRef)]

    override val arity: Int = 1
    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true
    override def tableNames(inputs: List[RelExpr]): List[String] = Nil
}

object Transform {
    sealed abstract class TransformType
    case object Join extends TransformType
    case object Union extends TransformType

    def apply(
        transformType: TransformType,
        transformer: Transformer,
        in: List[(String, ScalExpr)],
        partn: List[ScalExpr],
        order: List[SortExpr],
        out: List[(String, ColRef)]
    ): Transform = transformType match {
        case Join =>
            JoinTransform(transformer, in, partn, order, out)
        case Union =>
            UnionTransform(transformer, in, partn, order, out)
    }

    def buildExpr(
        transformType: TransformType,
        transformer: Transformer,
        in: List[(String, ScalExpr)],
        partn: List[ScalExpr],
        order: List[SortExpr],
        out: List[(String, ColRef)],
        input: RelExpr
    ): RelExpr = {
        val transformOp: Transform =
            apply(transformType, transformer, in, partn, order, out)

        val (inpSorted, rem) =
            input.resultOrder.span { se => partn contains se.expr }

        val unSorted: List[ScalExpr] = partn diff inpSorted.map(se => se.expr)

        if( unSorted.isEmpty && SortExpr.isSubsumedBy(order, rem) ) {
            RelOpExpr(transformOp, List(input))
        } else {
            // sort the input before applying the transform
            val reqInpSort: List[SortExpr] =
                inpSorted ::: unSorted.map(e => SortExpr(e)) ::: order
            val updInput: RelExpr = RelOpExpr(Order(reqInpSort), List(input))
            val transformExpr: RelExpr = RelOpExpr(transformOp, List(updInput))

            // restore the input order, if any
            val isResultOrderCompatible: Boolean = SortExpr.isSubsumedBy(
                input.resultOrder, transformExpr.resultOrder
            )

            if( isResultOrderCompatible ) transformExpr
            else RelOpExpr(Order(input.resultOrder), List(transformExpr))
        }
    }
}

sealed abstract class RetainTransform extends Transform {
    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs ::: out.map { case (_, col) => col }

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns :::
        out.map { case (_, col) => AnnotColRef(None, col.name) }
}

/** Computes a generic transform on the input;
  * the result is input left-outer-joined with the result
  * @param transformer Function to transform the input
  * @param in Transformation input parameters
  * @param partn Partition attribute for the input rows
  * @param order Needed order for the rows within each partn before processing
  * @param out Transformation output parameters
  */
case class JoinTransform(
    override val transformer: Transformer,
    override val in: List[(String, ScalExpr)],
    override val partn: List[ScalExpr],
    override val order: List[SortExpr],
    override val out: List[(String, ColRef)]
) extends RetainTransform {
    override val transformType: Transform.TransformType = Transform.Join

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head.resultOrder
}

/** Computes a generic transform on the input;
  * the result is outer-unioned with the input
  * @param transformer Function to transform the input
  * @param in Transformation input parameters
  * @param partn Partition attribute for the input rows
  * @param order Needed order for the rows within each partn before processing
  * @param out Transformation output parameters
  */
case class UnionTransform(
    override val transformer: Transformer,
    override val in: List[(String, ScalExpr)],
    override val partn: List[ScalExpr],
    override val order: List[SortExpr],
    override val out: List[(String, ColRef)]
) extends RetainTransform {
    override val transformType: Transform.TransformType = Transform.Union

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] = Nil
}
