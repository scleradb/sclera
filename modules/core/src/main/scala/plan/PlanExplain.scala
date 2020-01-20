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

package com.scleradb.plan

import scala.language.postfixOps

import com.scleradb.objects._
import com.scleradb.exec._
import com.scleradb.plan._

import com.scleradb.sql.datatypes._
import com.scleradb.sql.types._
import com.scleradb.sql.statements._
import com.scleradb.sql.objects._
import com.scleradb.sql.expr._
import com.scleradb.sql.plan._
import com.scleradb.sql.exec.ScalExprEvaluator

import com.scleradb.external.expr.ExternalSourceExpr
import com.scleradb.external.plan.ExternalSourceEvalPlan

import com.scleradb.analytics.ml.classifier.objects._
import com.scleradb.analytics.ml.classifier.expr.Classify
import com.scleradb.analytics.ml.classifier.plan.ClassifyEvalOp

import com.scleradb.analytics.ml.clusterer.objects._
import com.scleradb.analytics.ml.clusterer.expr.Cluster
import com.scleradb.analytics.ml.clusterer.plan.ClusterEvalOp

import com.scleradb.analytics.ml.imputer.expr.Impute
import com.scleradb.analytics.ml.imputer.plan.ImputeEvalOp

import com.scleradb.analytics.nlp.expr.NlpRelOp
import com.scleradb.analytics.nlp.plan.NlpEvalOp

import com.scleradb.analytics.sequence.matcher.plan.RowSequenceMatchEvalOp
import com.scleradb.analytics.sequence.matcher.aggregate._

import com.scleradb.analytics.transform.plan.TransformEvalOp
import com.scleradb.analytics.infertypes.plan.InferTypesEvalOp

private[scleradb]
class PlanExplain(
    normalizer: Normalizer,
    planner: Planner,
    evaluator: ScalExprEvaluator
) {
    def explain(relExpr: RelExpr): List[String] = {
        val normRelExpr: RelExpr = normalizer.normalizeRelExpr(relExpr)
        val plan: RelPlan = planner.planRelEval(normRelExpr)

        explain(plan)
    }

    def explain(plan: Plan): List[String] = plan match {
        case p@RelBaseExprPlan(tableRef: TableRefSource) => List(
            "[Table: " + tableRef.name + "]",
            p.locationIdOpt.map { locId =>
                "[Location: " + locId.repr + "]"
            } getOrElse "[Stream]"
        )

        case RelBaseExprPlan(values@Values(_, rows)) =>
            val header: String =
                "[Values " + values.name + "(" +
                    values.tableColRefs.map(col => col.repr).mkString(", ") +
                ")]"
            header::rows.map(row => "[Value: " + row.repr + "]")

        case p@RelEvalExprPlan(_, _, inputPlan) =>
            ("[Materialize Stream -> " + p.tableName + "]")::
            ("[Location: " + p.locationId.repr + "]")::
            explainIndent(List(inputPlan))

        case RelOpExprPlan(_, RegularRelExprOp(op, opDepsPlans), inputPlans) =>
            explainExprOp(op):::explainIndent(inputPlans:::opDepsPlans)

        case p@RelOpExprPlan(_, op: MaterializeRelExprOp, inputPlans) =>
            val locStr: String =
                p.locationIdOpt.map { locId =>
                    "[Location: " + locId.repr + "]"
                } getOrElse "[Stream]"

            ("[Materialize Expression -> " + op.tableName + "]")::locStr::
            explainIndent(inputPlans)

        case RelExprEvalPlan(_, _, inputPlan) =>
            "[Evaluate]"::explainIndent(List(inputPlan))

        case RelOpEvalPlan(_,
                RelEquiNestedLoopsJoinEvalOp(
                    evaluator, joinType, lhsCol, rhsCol, rhsPlan
                ),
                List(lhsPlan)) =>
            ("[Nested Loops Join (" + joinType + "): " +
            lhsCol.repr + " = " + rhsCol.repr + "]")::
            explainIndent(List(lhsPlan, rhsPlan))

        case RelOpEvalPlan(_, op: RelUnaryEvalOp,
                           inpPlans) =>
            explainEvalOp(op):::explainIndent(inpPlans:::op.opDepsPlans)

        case RelOpEvalPlan(_, op, inpPlans) =>
            explainEvalOp(op):::explainIndent(inpPlans)

        case RelValuesPlan(values@Values(_, rows)) =>
            val header: String =
                "[Values " + values.name + "(" +
                    values.tableColRefs.map(col => col.repr).mkString(", ") +
                ")]"
            header::rows.map(row => "[Value: " + row.repr + "]")

        case ExternalSourceEvalPlan(src@ExternalSourceExpr(_, source)) => List(
            Some("[" + source.toString + "]"),
            src.resultOrder match {
                case Nil => None
                case sortExprs => Some(
                    "[ORDERED BY " +
                    sortExprs.map(e => e.repr).mkString(", ") + "]"
                )
            }
        ) flatten

        case _ =>
            throw new IllegalArgumentException(
                "Explain not implemented for " + plan
            )
    }

    private def explainIndent(
        plans: List[Plan], bar: String = "|", cross: String = "+-"
    ): List[String] = plans match {
        case List(plan) =>
            bar::explain(plan)
        case _ =>
            val inits: List[String] = plans.tail.reverse.flatMap { plan =>
                val strs: List[String] = explain(plan)
                bar::(cross + strs.head)::strs.tail.map(str => bar + " " + str)
            }

            val last: List[String] = bar::explain(plans.head)

            inits:::last
    }

    private def explainExprOp(op: RelOp): List[String] = op match {
        case Project(targets) =>
            List("[Project: " + targets.map(t => t.repr).mkString(", ") + "]")

        case Aggregate(targets, groupExprs, predOpt) =>
            val targetStr: String =
                "[Aggregate: " + targets.map(t => t.repr).mkString(", ") + "]"

            val groupByStrs: List[String] = groupExprs match {
                case Nil => Nil
                case ges => List(
                    "[Group by: " + ges.map(e => e.repr).mkString(", ") + "]"
                )
            }

            val predStrs: List[String] =
                predOpt.toList.map(e => "[Having: " + e.repr + "]")

            targetStr::groupByStrs:::predStrs

        case Order(sortExprs) => List(
            "[Order by: " + sortExprs.map(e => e.repr).mkString(", ") + "]"
        )

        case LimitOffset(limitOpt, offset, order) =>
            val limitStrs: List[String] =
                limitOpt.toList.map(limit => "[Limit: " + limit + "]")
            val offsetStrs: List[String] =
                if( offset == 0 ) Nil else List("[Offset: " + offset + "]")
            val orderStrs: List[String] =
                if( order.isEmpty ) Nil else List(
                    "[Order by: " + order.map(e => e.repr).mkString(", ") + "]"
                )
            limitStrs:::offsetStrs:::orderStrs

        case DistinctOn(exprs, order) =>
            val distinctStr: String =
                "[Distinct On: " + exprs.map(e => e.repr).mkString(", ") + "]"
            val orderStrs: List[String] =
                if( order.isEmpty ) Nil else List(
                    "[Order by: " + order.map(e => e.repr).mkString(", ") + "]"
                )
            distinctStr::orderStrs

        case Distinct => List("[Distinct]")

        case Select(pred) => List("[Filter: " + pred.repr + "]")

        case Join(joinType, JoinOn(pred)) =>
            List("[Join (" + joinType + "): " + pred.repr + "]")

        case Compound(compoundType) => List("[" + compoundType + "]")
            
        case Join(_, _) | TableAlias(_, _, _) =>
            throw new RuntimeException("Found unnormalized operator " + op)

        case _ =>
            throw new IllegalArgumentException(
                "Explain not implemented for " + op
            )
    }

    private def explainEvalOp(op: RelEvalOp): List[String] = op match {
        case RelUnionEvalOp => explainExprOp(Compound(Union))

        case RelEquiMergeJoinEvalOp(_, joinType, lhsCol, rhsCol) =>
            List("[Merge Join (" + joinType + "): " +
                 lhsCol.repr + " = " + rhsCol.repr + "]")

        case (evalOp: RelUnaryBatchEvalOp) =>
            "[Batch Evaluation]"::explainExprOp(evalOp.op)

        case RelProjectEvalOp(_, targets, _) =>
            List("[Project: " + targets.map(t => t.repr).mkString(", ") + "]")

        case RelSelectEvalOp(_, pred, _) =>
            List("[Filter: " + pred.repr + "]")

        case RelDistinctEvalOp(_, Some(exprs), _) =>
            List("[Distinct On: " + exprs.map(e => e.repr).mkString(", ") + "]")

        case RelDistinctEvalOp(_, None, _) =>
            List("[Distinct]")

        case RelLimitOffsetEvalOp(limitOpt, offset, _) =>
            val limitStrs: List[String] =
                limitOpt.toList.map(limit => "[Limit: " + limit + "]")
            val offsetStrs: List[String] =
                if( offset == 0 ) Nil else List("[Offset: " + offset + "]")
            limitStrs:::offsetStrs

        case ClassifyEvalOp(_, Classify(classifierId, ColRef(cname))) =>
            List("[Classifier " + classifierId.name + " => " + cname + "]")

        case ClusterEvalOp(_, Cluster(clustererId, ColRef(cname))) =>
            List("[Clusterer " + clustererId.name + " => " + cname + "]")

        case ImputeEvalOp(_, Impute(imputeSpecs)) =>
            imputeSpecs.map { spec => "[" + spec.description + "]" }

        case NlpEvalOp(
                NlpRelOp(
                    libOpt, langOpt,
                    name, args, inputCol,
                    resultCols
                )
             ) =>
            (
                "[" + name +
                    "(" +
                        args.map(a => a.repr).mkString(", ") +
                        " ON " + inputCol.name +
                    ") => " +
                    "(" +
                        resultCols.map(col => col.repr).mkString(", ") +
                    ")" +
                "]"
            )::
            libOpt.toList.map(lib => "[Library: " + lib + "]"):::
            langOpt.toList.map(lang => "[Language: " + lang + "]")

        case RowSequenceMatchEvalOp(_, _, matchOp) =>
            ("[" + matchOp.labeler.repr + "]")::
            "[Matcher]"::matchOp.matcher.repr

        case AlignEvalOp(_, distanceExpr, marginOpt) =>
            ("[Align on " + distanceExpr.repr + "]")::
            marginOpt.toList.map(m => "[Margin = " + m + "]")

        case DisjointIntervalEvalOp(_, inpLhs, inpRhs, outLhs, outRhs, partn) =>
            val splitStr: String =
                "[Split intervals (" + inpLhs.repr + ", " + inpRhs.repr +
                ") -> (" + outLhs.repr + ", " + outRhs.repr + ")]"

            val partnStrs: List[String] = partn match {
                case Nil => Nil
                case partnCols =>
                    val partnStrs: List[String] = partnCols.map { c => c.repr }
                    List("[Partition by: " + partnStrs.mkString(", ") + "]")
            }

            splitStr::partnStrs

        case TransformEvalOp(_, op) =>
            List(
                "[Transform: " + op.transformer + "]",
                "[In: " + (
                    op.in.map { case (s, e) => s + " -> " + e.repr }
                 ).mkString(", ") + "]"
            ) ::: {
                if( op.partn.isEmpty ) Nil else List(
                    "[Partition: " + op.partn.mkString(", ") + "]"
                )
            } ::: List(
                "[Out: " + (
                    op.out.map { case (s, col) => s + " -> " + col.repr }
                 ).mkString(", ") + "]"
            )

        case InferTypesEvalOp(cols, nulls, lookAheadOpt) =>
            ("[InferTypes: " + cols.map(c => c.repr).mkString(", ") + "]") :: {
                if( nulls.isEmpty ) Nil else List(
                    "[Nulls: " + nulls.mkString(", ") + "]"
                )
            } ::: {
                lookAheadOpt.toList.map( lookAhead =>
                    "[LookAhead: " + lookAhead + "]"
                )
            }

        case UnPivotEvalOp(outValCol, outKeyCol, inColVals) =>
            ("[UnPivot: " + outValCol.repr + "]") ::
            ("[Key: " + outKeyCol.repr + "]") ::
            inColVals.map { case (_, v) => "[Value: " + v.repr + "]" }

        case OrderedByEvalOp(sortExprs) => List(
            "[Ordered by: " + sortExprs.map(e => e.repr).mkString(", ") + "]"
        )

        case _ =>
            throw new IllegalArgumentException(
                "Explain not implemented for " + op
            )
    }
}
