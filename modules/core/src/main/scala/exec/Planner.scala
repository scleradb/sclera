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

package com.scleradb.exec

import com.scleradb.util.tools.Counter

import com.scleradb.dbms.location.LocationId

import com.scleradb.sql.types.SqlType
import com.scleradb.sql.expr._
import com.scleradb.sql.plan._

import com.scleradb.external.expr.ExternalSourceExpr
import com.scleradb.external.plan.ExternalSourceEvalPlan

import com.scleradb.analytics.transform.expr.Transform
import com.scleradb.analytics.transform.plan.TransformEvalOp

import com.scleradb.analytics.infertypes.expr.InferTypes
import com.scleradb.analytics.infertypes.plan.InferTypesEvalOp

import com.scleradb.analytics.ml.expr.MLRelOp

import com.scleradb.analytics.ml.classifier.expr.Classify
import com.scleradb.analytics.ml.classifier.plan.ClassifyEvalOp

import com.scleradb.analytics.ml.clusterer.expr.Cluster
import com.scleradb.analytics.ml.clusterer.plan.ClusterEvalOp

import com.scleradb.analytics.ml.imputer.expr.Impute
import com.scleradb.analytics.ml.imputer.plan.ImputeEvalOp

import com.scleradb.analytics.sequence.matcher.expr.LabeledMatch
import com.scleradb.analytics.sequence.matcher.plan.RowSequenceMatchEvalOp

import com.scleradb.analytics.nlp.expr.NlpRelOp
import com.scleradb.analytics.nlp.plan.NlpEvalOp

private[scleradb]
class Planner(processor: Processor) {
    // plan the given expression
    def planRel(
        relExpr: RelExpr
    ): RelPlan =
        if( relExpr.locationIdOpt.isEmpty )
            planRelEval(relExpr)
        else planRelExpr(relExpr)

    // compute the result as a stream
    // if the result is materialized, evaluate
    def planRelEval(
        relExpr: RelExpr
    ): RelEvalPlan = planRelEvalOpt(relExpr) getOrElse {
        // compute evaluation plan by forcing evaluation of the root
        planRelExprOpt(relExpr) match {
            case Some(plan) => RelExprEvalPlan(processor, relExpr, plan)
            case None =>
                throw new IllegalArgumentException("Unable to plan evaluation")
        }
    }

    // compute a relational expression as a relational expression
    // if the result is in-memory, materialize
    def planRelExpr(
        relExpr: RelExpr,
        targetOpt: Option[LocationId] = None
    ): RelExprPlan = planRelExprOpt(relExpr, targetOpt) getOrElse {
        // compute evaluation plan by forcing materialization of the root
        planRelEvalOpt(relExpr) match {
            case Some(plan) => RelEvalExprPlan(processor, targetOpt, plan)
            case None =>
                throw new IllegalArgumentException("Unable to plan expression")
        }
    }

    // compute an in-memory relation stream
    private def planRelEvalOpt(
        relExpr: RelExpr
    ): Option[RelEvalPlan] = relExpr match {
        case _ if !relExpr.isEvaluable =>
            None

        case (values: Values) =>
            Some(RelValuesPlan(values))

        case (extSrcExpr: ExternalSourceExpr) =>
            Some(ExternalSourceEvalPlan(extSrcExpr))

        case RelOpExpr(op@TableAlias(_, Nil, _), List(input), _) =>
            // normalization makes all column names distinct
            // so, can safely ignore table aliases
            planRelEvalOpt(input)

        case RelOpExpr(order: Order, List(input), _)
        if order.isSubsumedBy(input.resultOrder) =>
            planRelEvalOpt(input)

        case RelOpExpr(op@Join(joinType,
                               JoinOn(ScalOpExpr(Equals,
                                                 List(a: ColRef, b: ColRef)))),
                       inputs@List(lhs, rhs), _)
        if relExpr.isStreamEvaluable =>
            lhs.resultOrder match {
                case SortExpr(lcol: ColRef, ldir, lnord)::_
                if lcol == a =>
                    // lhs ordered on joining col
                    val rewriteRhs: RelExpr = rhs.resultOrder match {
                        case SortExpr(rcol: ColRef, rdir, rnord)::_
                        if (rcol == b) && (rdir == ldir) && (rnord == lnord) =>
                            // rhs already ordered on joining col
                            rhs
                        case _ =>
                            // rhs not ordered on joining col, needs sort
                            RelOpExpr(
                                Order(List(SortExpr(b, ldir, lnord))),
                                List(rhs)
                            )
                    }

                    val lhsPlan: RelEvalPlan = planRelEval(lhs)
                    val rhsPlan: RelEvalPlan = planRelEval(rewriteRhs)
                    val plan: RelEvalPlan = RelOpEvalPlan(
                        relExpr,
                        RelEquiMergeJoinEvalOp(
                            processor.scalExprEvaluator, joinType, a, b
                        ),
                        List(lhsPlan, rhsPlan)
                    )

                    Some(plan)

                case _ if (joinType == LeftOuter) || (joinType == Inner) =>
                    // lhs not ordered on joining col
                    // cannot sort, so perform a nested loops join
                    // with the lhs as the outer input
                    // => can only compute a leftouter or inner join
                    val lhsPlan: RelEvalPlan = planRelEval(lhs)
                    val rhsPlan: RelExprPlan = planRelExpr(rhs) match {
                        case (basePlan: RelBaseExprPlan) => basePlan
                        case (matPlan: RelEvalExprPlan) => matPlan
                        case plan@RelOpExprPlan(
                            _, RegularRelExprOp(Project(_) | Select(_), Nil),
                            List(_: RelBaseExprPlan)
                        ) => plan
                        case plan =>
                            val tableName: String = Counter.nextSymbol("XM")
                            RelOpExprPlan(
                                rhs,
                                MaterializeRelExprOp(processor, tableName),
                                List(plan)
                            )
                    }

                    val plan: RelEvalPlan =
                        RelOpEvalPlan(
                            relExpr,
                            RelEquiNestedLoopsJoinEvalOp(
                                processor, joinType, a, b, rhsPlan
                            ),
                            List(lhsPlan)
                        )
                    Some(plan)

                case _ => None
            }

        case RelOpExpr(Join(joinType,
                            JoinOn(ScalOpExpr(Equals,
                                              List(a: ColRef, b: ColRef)))),
                       inputs@List(lhs, _), _)
        if( lhs.locationIdOpt.isEmpty ) =>
            // change the equality order
            val joinPredRev: JoinPred =
                JoinOn(ScalOpExpr(Equals, List(b, a)))

            val joinVarA: Join = Join(joinType, joinPredRev)
            if( joinVarA.isStreamEvaluable(inputs) ) // avoid infinite recursion
                planRelEvalOpt(RelOpExpr(joinVarA, inputs))
            else None

        case RelOpExpr(Join(joinType,
                            joinPred@JoinOn(
                                ScalOpExpr(Equals,
                                           List(a: ColRef, b: ColRef)))),
                       List(lhs, rhs), _)
        if( !lhs.locationIdOpt.isEmpty && rhs.locationIdOpt.isEmpty ) =>
            // change the input order
            val inputsRev: List[RelExpr] = List(rhs, lhs)
            val joinTypeRev: JoinType = joinType match {
                case RightOuter => LeftOuter
                case LeftOuter => RightOuter
                case other => other
            }

            val joinVarB: Join = Join(joinTypeRev, joinPred)

            planRelEvalOpt(RelOpExpr(joinVarB, inputsRev))

        case RelOpExpr(op: RegularRelOp, inputs, _) =>
            val (preparedOp, opDepsPlans) = planRegularRelOperator(op)
            planRegularRelOpEvalOpt(preparedOp, opDepsPlans, inputs).map {
                case ((planOp, inputPlans)) =>
                    RelOpEvalPlan(relExpr, planOp, inputPlans)
            }

        case RelOpExpr(matchOp@LabeledMatch(labeler, _), List(input), _) =>
            val matchEvalOp: RelEvalOp =
                RowSequenceMatchEvalOp(
                    processor.scalExprEvaluator, labeler.rowLabels, matchOp
                )
            val labeledExpr: RelExpr = labeler.labeledExpr(input)
            val seqEvalPlan: RelEvalPlan = planRelEval(labeledExpr)
            Some(RelOpEvalPlan(relExpr, matchEvalOp, List(seqEvalPlan)))

        case RelOpExpr(EvaluateOp, List(input), _) =>
            if( input.locationIdOpt.isEmpty ) planRelEvalOpt(input)
            else planRelExprOpt(input).map { plan =>
                RelExprEvalPlan(processor, relExpr, plan)
            }

        case RelOpExpr(op: MLRelOp, inputs, _) =>
            val preparedOp: RelEvalOp = planMLRelOperator(op)
            val inputPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }
            Some(RelOpEvalPlan(relExpr, preparedOp, inputPlans))

        case RelOpExpr(op: NlpRelOp, inputs, _) =>
            val inputPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }
            Some(RelOpEvalPlan(relExpr, NlpEvalOp(op), inputPlans))

        case RelOpExpr(op: Align, inputs, _) =>
            val inputPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }

            val evalOp: AlignEvalOp =
                AlignEvalOp(
                    processor.scalExprEvaluator, op.distanceExpr, op.marginOpt
                )

            Some(RelOpEvalPlan(relExpr, evalOp, inputPlans))

        case RelOpExpr(op: DisjointInterval, inputs, _) =>
            val inputPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }

            val evalOp: DisjointIntervalEvalOp =
                DisjointIntervalEvalOp(
                    processor.scalExprEvaluator,
                    op.inpLhsColRef, op.inpRhsColRef,
                    op.outLhsColRef, op.outRhsColRef,
                    op.partnColRefs
                )

            Some(RelOpEvalPlan(relExpr, evalOp, inputPlans))

        case RelOpExpr(transform: Transform, inputs, _) =>
            val inputPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }

            val evalOp: TransformEvalOp =
                TransformEvalOp(processor.scalExprEvaluator, transform)

            Some(RelOpEvalPlan(relExpr, evalOp, inputPlans))

        case RelOpExpr(inferTypes: InferTypes, inputs, _) =>
            val inputPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }

            val evalOp: InferTypesEvalOp = InferTypesEvalOp(
                inferTypes.cols, inferTypes.nulls, inferTypes.lookAheadOpt
            )

            Some(RelOpEvalPlan(relExpr, evalOp, inputPlans))

        case RelOpExpr(unPivot: UnPivot, inputs, _) =>
            val inputPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }

            val evalOp: UnPivotEvalOp = UnPivotEvalOp(
                unPivot.outValCol, unPivot.outKeyCol, unPivot.inColVals
            )

            Some(RelOpEvalPlan(relExpr, evalOp, inputPlans))

        case RelOpExpr(OrderedBy(sortExprs), List(input), _)
        if SortExpr.isSubsumedBy(sortExprs, input.resultOrder) =>
            planRelEvalOpt(input)

        case RelOpExpr(OrderedBy(sortExprs), inputs, _) =>
            val inputPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }

            val evalOp: OrderedByEvalOp = OrderedByEvalOp(sortExprs)
            Some(RelOpEvalPlan(relExpr, evalOp, inputPlans))

        case _ => None
    }
 
    private def planRegularRelOpEvalOpt(
        preparedOp: RegularRelOp,
        opDepsPlans: List[RelExprPlan],
        inputs: List[RelExpr]
    ): Option[(RelEvalOp, List[RelEvalPlan])] = (preparedOp, inputs) match {
        case (op: Select, List(input))
        if op.isStreamEvaluable(inputs) =>
            val inpPlan: RelEvalPlan = planRelEval(input)
            if( op.isStreamEvaluable(inputs) ) {
                val planOp: RelEvalOp =
                    RelSelectEvalOp(
                        processor.scalExprEvaluator, op.predExpr, opDepsPlans
                    )
                Some((planOp, List(inpPlan)))
            } else if( op.isLocEvaluable(processor.dataCacheLocation) ) {
                val planOp: RelEvalOp =
                    RelUnaryFixedSizeBatchEvalOp(processor, op, opDepsPlans)
                Some((planOp, List(inpPlan)))
            } else None

        case (op: Project, List(input)) =>
            val inpPlan: RelEvalPlan = planRelEval(input)
            if( op.isStreamEvaluable(inputs) ) {
                val planOp: RelEvalOp =
                    RelProjectEvalOp(
                        processor.scalExprEvaluator, op.targetExprs, opDepsPlans
                    )
                Some((planOp, List(inpPlan)))
            } else if( op.isLocEvaluable(processor.dataCacheLocation) ) {
                val planOp: RelEvalOp =
                    RelUnaryFixedSizeBatchEvalOp(processor, op, opDepsPlans)
                Some((planOp, List(inpPlan)))
            } else None

        case (op: Aggregate, List(input))
        if( op.isLocEvaluable(processor.dataCacheLocation)) =>
            val partnExprs: List[ScalExpr] = op.streamPartitionExprs(inputs)
            if( partnExprs.isEmpty && !op.locationIdOpt(inputs).isEmpty )
                None
            else {
                val inpPlan: RelEvalPlan = planRelEval(input)
                val planOp: RelEvalOp =
                    RelUnaryPartitionBatchEvalOp(
                        processor, op, opDepsPlans, partnExprs
                    )

                Some((planOp, List(inpPlan)))
            }

        case (op@Compound(Union), inputs)
        if op.isStreamEvaluable(inputs) =>
            val inpPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }
            Some((RelUnionEvalOp, inpPlans))

        case (op@LimitOffset(limitOpt, offset, _), inputs)
        if op.isStreamEvaluable(inputs) =>
            val inpPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }
            val planOp: RelEvalOp =
                RelLimitOffsetEvalOp(limitOpt, offset, opDepsPlans)

            Some((planOp, inpPlans))

        case (op@DistinctOn(exprs, _), inputs)
        if op.isStreamEvaluable(inputs) =>
            val inpPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }
            val planOp: RelEvalOp =
                RelDistinctEvalOp(
                    processor.scalExprEvaluator, Some(exprs), opDepsPlans
                )

            Some((planOp, inpPlans))

        case (op@Distinct, inputs)
        if op.isStreamEvaluable(inputs) =>
            val inpPlans: List[RelEvalPlan] =
                inputs.map { input => planRelEval(input) }
            val planOp: RelEvalOp =
                RelDistinctEvalOp(
                    processor.scalExprEvaluator, None, opDepsPlans
                )

            Some((planOp, inpPlans))

        case _ => None
    }

    // compute a relational expression at a location
    private def planRelExprOpt(
        relExpr: RelExpr,
        targetOpt: Option[LocationId] = None
    ): Option[RelExprPlan] = relExpr match {
        case _
        if relExpr.locationIdOpt.isEmpty ||
           (!targetOpt.isEmpty && (relExpr.locationIdOpt != targetOpt)) =>
            None

        case (baseRelExpr: RelBaseExpr) =>
            Some(RelBaseExprPlan(baseRelExpr))

        case RelOpExpr(op@TableAlias(_, Nil, _), List(input), _) =>
            // normalization makes all column names distinct
            // so, can safely ignore table aliases
            planRelExprOpt(input, targetOpt)

        case RelOpExpr(op: RegularRelOp, inputs, _) =>
            val (preparedOp, opDepsPlans) = planRegularRelOperator(op)
            val planOp: RegularRelExprOp =
                RegularRelExprOp(preparedOp, opDepsPlans)

            val inputPlans: List[RelExprPlan] = inputs.map { input =>
                planRelExpr(input, relExpr.locationIdOpt)
            }

            Some(RelOpExprPlan(relExpr, planOp, inputPlans))

        case _ => None
    }

    private def planMLRelOperator(
        op: MLRelOp
    ): RelEvalOp = op match {
        case (classify: Classify) => ClassifyEvalOp(processor.schema, classify)
        case (cluster: Cluster) => ClusterEvalOp(processor.schema, cluster)
        case (impute: Impute) => ImputeEvalOp(processor.schema, impute)
    }

    private def planRegularRelOperator(
        op: RegularRelOp
    ): (RegularRelOp, List[RelExprPlan]) = op match {
        case Project(targets) =>
            val (preparedTargets, targetPlans) = planTargetExprs(targets)
            val preparedProject: Project = Project(preparedTargets)
            (preparedProject, targetPlans)

        case Aggregate(targetExprs, groupExprs, predOpt) =>
            val (preparedTargets, targetPlans) =
                planTargetExprs(targetExprs)

            val (preparedGroupExprs, groupPlans) =
                planScalExprs(groupExprs)

            val (preparedPredOpt, predPlans) = planScalExprOpt(predOpt)

            val preparedAggregate: Aggregate =
                Aggregate(preparedTargets, preparedGroupExprs, preparedPredOpt)
            val aggregatePlans: List[RelExprPlan] =
                targetPlans:::groupPlans:::predPlans

            (preparedAggregate, aggregatePlans)

        case Order(sortExprs) =>
            val (preparedSortExprs, sortPlans) = planSortExprs(sortExprs)
            val preparedOrder: Order = Order(preparedSortExprs)

            (preparedOrder, sortPlans)

        case LimitOffset(limitOpt, offset, order) =>
            val (preparedOrder, orderPlans) = planSortExprs(order)

            val preparedLimitOffset: LimitOffset =
                LimitOffset(limitOpt, offset, preparedOrder)

            (preparedLimitOffset, orderPlans)

        case DistinctOn(exprs, order) =>
            val (preparedExprs, exprPlans) = planScalExprs(exprs)
            val (preparedOrder, orderPlans) = planSortExprs(order)

            val preparedDistinctOn: DistinctOn =
                DistinctOn(preparedExprs, preparedOrder)
            val distinctOnPlans: List[RelExprPlan] = exprPlans:::orderPlans

            (preparedDistinctOn, distinctOnPlans)

        case Distinct => (Distinct, Nil)

        case Select(pred) =>
            val (preparedPred, predPlans) = planScalExpr(pred)
            val preparedSelect: Select = Select(preparedPred)

            (preparedSelect, predPlans)

        case Join(joinType, joinPred) =>
            val (preparedJoinPred, predPlans) = planJoinPred(joinPred)
            val preparedJoin: Join = Join(joinType, preparedJoinPred)

            (preparedJoin, predPlans)

        case compoundOp@Compound(_) => (compoundOp, Nil)

        case _ =>
            throw new RuntimeException("Found unnormalized operator " + op)
    }

    private def planJoinPred(
        joinPred: JoinPred
    ): (JoinPred, List[RelExprPlan]) = joinPred match {
        case JoinOn(pred) =>
            val (preparedPred, plans) = planScalExpr(pred)
            (JoinOn(preparedPred), plans)

        case joinUsing@JoinUsing(_) => (joinUsing, Nil)

        case JoinNatural => (JoinNatural, Nil)
    }

    private def planTargetExprs(
        targetExprs: List[ScalarTarget]
    ): (List[ScalarTarget], List[RelExprPlan]) = {
        val targetEval: List[(ScalarTarget, List[RelExprPlan])] =
            targetExprs.map { targetExpr => planTargetExpr(targetExpr) }

        val (preparedTargetExprs, targetPlansList) = targetEval.unzip
        (preparedTargetExprs, targetPlansList.flatten)
    }

    private def planTargetExpr(
        targetExpr: ScalarTarget
    ): (ScalarTarget, List[RelExprPlan]) = targetExpr match {
        case AliasedExpr(expr, alias) =>
            val (preparedExpr, plans) = planScalExpr(expr)
            (AliasedExpr(preparedExpr, alias), plans)
        case other => (other, Nil)
    }

    private def planSortExprs(
        sortExprs: List[SortExpr]
    ): (List[SortExpr], List[RelExprPlan]) = {
        val sortEval: List[(SortExpr, List[RelExprPlan])] =
            sortExprs.map { sortExpr => planSortExpr(sortExpr) }

        val (preparedSortExprs, sortPlansList) = sortEval.unzip
        (preparedSortExprs, sortPlansList.flatten)
    }

    private def planSortExpr(
        sortExpr: SortExpr
    ): (SortExpr, List[RelExprPlan]) = sortExpr match {
        case SortExpr(expr, sortDir, nullsOrder) =>
            val (preparedExpr, plans) = planScalExpr(expr)
            (SortExpr(preparedExpr, sortDir, nullsOrder), plans)
    }

    private def planScalExprs(
        scalExprs: List[ScalExpr]
    ): (List[ScalExpr], List[RelExprPlan]) = {
        val scalEval: List[(ScalExpr, List[RelExprPlan])] =
            scalExprs.map { scalExpr => planScalExpr(scalExpr) }

        val (preparedScalExprs, scalPlansList) = scalEval.unzip
        (preparedScalExprs, scalPlansList.flatten)
    }

    private def planScalExprOpt(
        scalExprOpt: Option[ScalExpr]
    ): (Option[ScalExpr], List[RelExprPlan]) = scalExprOpt match {
        case Some(expr) =>
            val (preparedExpr, plans) = planScalExpr(expr)
            (Some(preparedExpr), plans)
        case None => (None, Nil)
    }

    private def planScalExpr(
        scalExpr: ScalExpr
    ): (ScalExpr, List[RelExprPlan]) = scalExpr match {
        case ScalOpExpr(op, inputs) =>
            val (preparedInputs, plans) = planScalExprs(inputs)
            val preparedScalOpExpr: ScalOpExpr = ScalOpExpr(op, preparedInputs)
            (preparedScalOpExpr, plans)

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            val (preparedArgExpr, argExprPlans) = planScalExpr(argExpr)

            val (whens, thens) = whenThen.unzip
            val (preparedWhens, whenPlans) = planScalExprs(whens)
            val (preparedThens, thenPlans) = planScalExprs(thens)

            val preparedWhenThen: List[(ScalExpr, ScalExpr)] =
                preparedWhens zip preparedThens

            val whenThenPlans: List[RelExprPlan] = whenPlans:::thenPlans

            val (preparedDefaultExpr, defaultExprPlans) =
                planScalExpr(defaultExpr)

            val preparedCaseExpr: CaseExpr =
                CaseExpr(preparedArgExpr, preparedWhenThen, preparedDefaultExpr)
            val plans: List[RelExprPlan] =
                argExprPlans:::whenThenPlans:::defaultExprPlans

            (preparedCaseExpr, plans)

        case (colRef: ColRef) => (colRef, Nil)

        case (annotColRef: AnnotColRef) => (annotColRef, Nil)

        case (value: ScalValue) => (value, Nil)

        case ScalSubQuery(relExpr) =>
            val plan: RelExprPlan = planRelExpr(relExpr)
            val preparedScalSubQuery: ScalSubQuery =
                ScalSubQuery(plan.result.relExpr)

            (preparedScalSubQuery, List(plan))

        case Exists(relExpr) =>
            val plan: RelExprPlan = planRelExpr(relExpr)
            val preparedExists: Exists = Exists(plan.result.relExpr)

            (preparedExists, List(plan))

        case ScalCmpRelExpr(qual, subQueryOrList) =>
            val (preparedSubQueryOrList, plans) =
                planRelSubQuery(subQueryOrList)
            val preparedScalCmpRelExpr: ScalCmpRelExpr =
                ScalCmpRelExpr(qual, preparedSubQueryOrList)

            (preparedScalCmpRelExpr, plans)

        case reject =>
            throw new RuntimeException(
                "Expression not normalized: " + reject
            )
    }

    private def planRelSubQuery(
        subQueryOrList: RelSubQueryBase
    ): (RelSubQueryBase, List[RelExprPlan]) = subQueryOrList match {
        case scalarList@ScalarList(_) => (scalarList, Nil)
        case RelSubQuery(relExpr) =>
            val plan: RelExprPlan = planRelExpr(relExpr)
            (RelSubQuery(plan.result.relExpr), List(plan))
    }
}
