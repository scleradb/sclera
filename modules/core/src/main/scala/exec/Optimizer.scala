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

import scala.language.postfixOps

import com.scleradb.dbms.location.{Location, LocationId}
import com.scleradb.sql.expr._

import com.scleradb.analytics.ml.classifier.expr.Classify
import com.scleradb.analytics.ml.clusterer.expr.Cluster
import com.scleradb.analytics.ml.imputer.expr.Impute

import com.scleradb.analytics.sequence.labeler.ConstRowLabeler

import com.scleradb.analytics.sequence.matcher.LabelSequenceMatcher
import com.scleradb.analytics.sequence.matcher.aggregate._
import com.scleradb.analytics.sequence.matcher.expr.LabeledMatch

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.util.tools.Counter

/** Rule-based relational expression optimizer */
object Optimizer {
    def optimizeRelExpr(relExpr: RelExpr, n: Int = 0): RelExpr =
        if( n < 100 ) { // capping the number of invocations at 100
            val optRelExpr: RelExpr = optimizeIterRelExpr(relExpr)
            if( optRelExpr != relExpr ) optimizeRelExpr(optRelExpr, n+1)
            else optRelExpr
        } else relExpr

    private def optimizeIterRelExpr(relExpr: RelExpr): RelExpr = relExpr match {
        case (baseRelExpr: RelBaseExpr) => applyRules(baseRelExpr)

        case RelOpExpr(op, inputs, locIdOpt) =>
            val updInputs: List[RelExpr] =
                inputs.map { input => optimizeIterRelExpr(input) }

            val updExpr: RelExpr = RelOpExpr(op, updInputs, locIdOpt) 
            applyRules(updExpr)

        // TODO: temporary patch -- ignoring other exprs
        case _ => relExpr
    }

    private val rules: List[RelExpr => Option[RelExpr]] = List(
        nodisplay("InsertEvaluate", insertEvaluate),
        nodisplay("RemoveTableAlias", removeTableAlias),
        nodisplay("PredicatePushDown", predicatePushDown),
        nodisplay("ProjectPushDown", projectPushDown),
        nodisplay("MergeProjects", mergeProjects),
        nodisplay("RemoveProject", removeProject),
        nodisplay("RemoveOrder", removeOrder),
        nodisplay("RemoveEvals", removeEvals),
        nodisplay("EvalPushDown", evalPushDown),
        nodisplay("RewriteAggregate", rewriteAggregate)
    )

    private def applyRules(relExpr: RelExpr): RelExpr =
        rules.foldLeft (relExpr) {
            case (expr, rule) => rule(expr) getOrElse expr
        }

    private def removeTableAlias(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(_: TableAlias, List(input), _) => Some(input)
        case _ => None
    }

    private def mergeProjects(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(u@Project(targetExprsUpper),
                       List(RelOpExpr(l@Project(targetExprsLower),
                            input, lLocIdOpt)),
                       uLocIdOpt)
        if u.isStreamEvaluable(input) && l.isStreamEvaluable(input) &&
            (uLocIdOpt.isEmpty || lLocIdOpt.isEmpty) =>
            val replMap: Map[ColRef, ScalExpr] = Map() ++
                targetExprsLower.map { t => (t.alias -> t.expr) }

            val updTargetExprs: List[ScalarTarget] = targetExprsUpper.map { t =>
                ScalarTarget(ScalExpr.replace(t.expr, replMap), t.alias)
            }

            val locIdOpt: Option[LocationId] = uLocIdOpt orElse lLocIdOpt
            Some(RelOpExpr(Project(updTargetExprs), input, locIdOpt))

        case _ => None
    }

    private def removeProject(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(Project(targetExprs), List(input), _)
        if relExpr.tableColRefs == input.tableColRefs =>
            val isSelfMap: Boolean =
                targetExprs.forall { t => t.expr == t.alias }

            if( isSelfMap ) Some(input) else None

        case _ =>
            None
    }

    private def removeOrder(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(order: Order, List(input), _)
        if order.isSubsumedBy(input.resultOrder) =>
            Some(input) // order redundant

        case RelOpExpr(op: Order, List(RelOpExpr(_: Order, input, _)), lOpt) =>
            Some(RelOpExpr(op, input, lOpt))

        case RelOpExpr(EvaluateOp, List(
                RelOpExpr(order: Order, List(
                    RelOpExpr(op@(Project(_) | Select(_)), List(
                        RelOpExpr(_: Order, input, innerLocOpt) // redundant
                    ), projLocOpt)
                ), outerLocOpt)
        ), _) if(
            innerLocOpt.forall(
                locId => op.isLocEvaluable(locId.location(input.head.schema))
            )
        ) => 
            Some(
                RelOpExpr(EvaluateOp, List(
                    RelOpExpr(order, List(
                        RelOpExpr(op, input, projLocOpt orElse innerLocOpt)
                    ), outerLocOpt)
                ))
            )

        case _ => None
    }

    private def predicatePushDown(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(Select(predExpr),
                       List(RelOpExpr(project@Project(targetExprs),
                                      inputs, lLocIdOpt)),
                       uLocIdOpt)
        if uLocIdOpt.isEmpty || lLocIdOpt.isEmpty =>
            val targetMap: Map[ColRef, ScalExpr] = Map() ++
                targetExprs.map { t => (t.alias -> t.expr) }

            val updPredExpr: ScalExpr = ScalExpr.replace(predExpr, targetMap)
            val locIdOpt: Option[LocationId] = uLocIdOpt orElse lLocIdOpt
            Some(RelOpExpr(project,
                 List(RelOpExpr(Select(updPredExpr), inputs))))

        case RelOpExpr(Select(selectPred),
                       List(RelOpExpr(Join(Inner, JoinOn(jPred)),
                                      inputs, lLocIdOpt)),
                       uLocIdOpt)
        if uLocIdOpt.isEmpty || lLocIdOpt.isEmpty =>
            val pred: ScalExpr = conjunctsPred(List(selectPred, jPred))
            Some(RelOpExpr(Join(Inner, JoinOn(pred)), inputs))

       case RelOpExpr(Join(Inner, JoinOn(joinPred)), inputs, None)
       if( joinPred != BoolConst(true) ) =>
            val conjuncts: List[ScalExpr] = predConjuncts(joinPred)

            val (remConjuncts, updInputs, isModified) =
                inputs.reverse.foldLeft (conjuncts, List[RelExpr](), false) {
                    case ((rem, prevInputs, prevIsModified), input) =>
                        val (pushDown, others) = rem.partition { conjunct =>
                            colRefs(conjunct).diff(input.tableColRefs).isEmpty
                        }

                        if( pushDown.isEmpty ) // nothing to push down
                            (rem, input::prevInputs, prevIsModified)
                        else {
                            val pushedPredicate: ScalExpr =
                                conjunctsPred(pushDown)
                            val updInput: RelExpr =
                                RelOpExpr(Select(pushedPredicate), List(input))

                            (others, updInput::prevInputs, true)
                        }
                }

            if( isModified ) {
                val updJoinPred: ScalExpr = conjunctsPred(remConjuncts)
                Some(RelOpExpr(Join(Inner, JoinOn(updJoinPred)),
                               updInputs))
            } else None

        case RelOpExpr(select@Select(selectPred),
                       List(RelOpExpr(op@Classify(_, col), inputs, _)),
                       locIdOpt)
        if !(selectPred.colRefs contains col) =>
            Some(RelOpExpr(op, List(RelOpExpr(select, inputs, locIdOpt))))

        case RelOpExpr(select@Select(selectPred),
                       List(RelOpExpr(op@Cluster(_, col), inputs, _)), locIdOpt)
        if !(selectPred.colRefs contains col) =>
            Some(RelOpExpr(op, List(RelOpExpr(select, inputs, locIdOpt))))

        case RelOpExpr(select@Select(selectPred),
                       List(RelOpExpr(op@Impute(specs), inputs, _)), locIdOpt)
        if selectPred.colRefs.intersect(op.addedCols.toSet).isEmpty =>
            Some(RelOpExpr(op, List(RelOpExpr(select, inputs, locIdOpt))))

        case RelOpExpr(select: Select,
                       List(RelOpExpr(EvaluateOp, inps@List(inp), None)), _) =>
            val isPushable: Boolean = inp.locationIdOpt match {
                case Some(locId) =>
                    select.isLocEvaluable(locId.location(inp.schema))
                case None =>
                    select.isStreamEvaluable(inps)
            }

            if( isPushable )
                Some(RelOpExpr(EvaluateOp, List(RelOpExpr(select, inps))))
            else None

        case _ => None
    }

    private def removeEvals(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(EvaluateOp, List(input), _)
        if input.locationIdOpt.isEmpty => Some(input)

        case RelOpExpr(op, List(RelOpExpr(EvaluateOp, inps@List(inp), _)), _) =>
            inp.locationIdOpt.flatMap { locId =>
                if( op.isLocEvaluable(locId.location(inp.schema)) ) {
                    val rewrite: RelExpr = RelOpExpr(op, inps, None)
                    if( rewrite.resultOrder == relExpr.resultOrder )
                        Some(rewrite)
                    else None
                } else None
            }

        case _ => None
    }

    private def evalPushDown(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(EvaluateOp,
                       List(RelOpExpr(project: Project, inputs@List(input),
                                      None)), None)
        if input.tableColRefs.size < relExpr.tableColRefs.size &&
           !project.isComplexTarget =>
            val updInputs: List[RelExpr] = List(RelOpExpr(EvaluateOp, inputs))
            if( project.isStreamEvaluable(updInputs) )
                Some(RelOpExpr(project, updInputs))
            else None

        case _ => None
    }

    private def projectPushDown(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(proj: Project,
                       List(RelOpExpr(EvaluateOp, inputs@List(input), None)),
                       None)
        if input.tableColRefs.size >= relExpr.tableColRefs.size &&
           !proj.isComplexTarget && input.resultOrder.isEmpty =>
            input.locationIdOpt match {
                case Some(locId)
                if( proj.isLocEvaluable(locId.location(input.schema)) ) =>
                    Some(RelOpExpr(EvaluateOp, List(RelOpExpr(proj, inputs))))
                case None => // evaluation redundant
                    Some(RelOpExpr(proj, inputs))
                case _ =>
                    None
            }

        case _ => None
    }

    private def insertEvaluate(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(EvaluateOp, _, _) => None

        case RelOpExpr(op, inputs, locIdOverrideOpt)
        if inputs.forall { input => input.isEvaluable } =>
            val updInputs: List[RelExpr] = inputs.map { input =>
                (relExpr.locationIdOpt, input.locationIdOpt) match {
                    case (_, None) =>
                        input
                    case (Some(locId), Some(inpLocId)) if locId == inpLocId =>
                        input
                    case _ =>
                        RelOpExpr(EvaluateOp, List(input))
                }
            }

            if( inputs == updInputs ) None else {
                Some(RelOpExpr(op, updInputs, locIdOverrideOpt))
            }

        case _ => None
    }

    private def rewriteAggregate(
        relExpr: RelExpr
    ): Option[RelExpr] = relExpr match {
        case RelOpExpr(op@Aggregate(targets, groupExprs, predOpt), inputs, None)
        if relExpr.locationIdOpt.isEmpty && relExpr.isStreamEvaluable &&
           !op.requiresSort(inputs) =>
            val (updTargets, targetAggrMap) = extractAggregates(targets)

            val (updPredOpt, aggrMap) = predOpt match {
                case Some(pred) =>
                    val (updPred, predAggrMap) =
                        extractAggregates(pred, targetAggrMap)
                    (Some(updPred), predAggrMap)
                case None => (None, targetAggrMap)
            }

            val aggrInfo: List[((AggregateFunction, List[ScalExpr]), ColRef)] =
                aggrMap.toList
            val aggrInps: List[ScalExpr] =
                aggrInfo.flatMap { case ((_, inp), _) => inp } distinct
            val aliasedAggrInputs: List[ScalarTarget] =
                aggrInps.flatMap {
                    case (v: ScalColValue) => // do not replace the constants
                        None
                    case (col: ColRef) =>
                        Some(RenameCol(col, col))
                    case expr =>
                        Some(AliasedExpr(expr, ColRef(Counter.nextSymbol("A"))))
                }

            val aggrInpAliasMap: Map[ScalExpr, ColRef] = Map() ++
                aliasedAggrInputs.map { t => t.expr -> t.alias }

            val aliasedGroupExprs: List[RenameCol] =
                groupExprs.map {
                    case (col: ColRef) => RenameCol(col, col)
                    case other =>
                        throw new RuntimeException(
                            "Found unnormalized GROUP BY: " + other.repr
                        )
                }

            val inputCols: List[RenameCol] = {
                targets.flatMap { t =>
                    t.expr.scalarCols.map { col => RenameCol(col, col) }
                } :::
                predOpt.toList.flatMap { expr =>
                    expr.scalarCols.map { col => RenameCol(col, col) }
                }
            } distinct

            // add a project to the input, if not redundant
            val lowerProjTargets: List[ScalarTarget] =
                (aliasedAggrInputs:::aliasedGroupExprs:::inputCols).distinct

            val updInputs: List[RelExpr] =
                if( lowerProjTargets.forall { t => t.expr == t.alias } &&
                    (lowerProjTargets.size == inputs.head.tableColRefs.size) )
                    inputs
                else List(RelOpExpr(Project(lowerProjTargets), inputs))

            val aggrSpecs: List[SeqFunctionSpec] =
                aggrInfo.map { case ((f, inps), alias) =>
                    val inpAliasList: List[ScalExpr] = inps.map { inp =>
                        aggrInpAliasMap.get(inp) getOrElse inp
                    }

                    SeqFunctionSpec(alias, f.name, inpAliasList)
                }

            val groupCols: List[ColRef] =
                aliasedGroupExprs.map { t => t.alias }

            val retainedInpCols: List[ColRef] =
                inputCols.map { case (t: RenameCol) => t.alias }

            val matcherColSpec: SeqAggregateColSetSpec =
                SeqAggregateColSetSpec(
                    aggrSpecs, (groupCols:::retainedInpCols).distinct
                )

            val labeler: ConstRowLabeler = ConstRowLabeler(Label("X"))
            val matcher: LabelSequenceMatcher =
                LabelSequenceMatcher(
                    matcherColSpec, groupCols, false, false
                )

            val matchExpr: RelExpr =
                RelOpExpr(LabeledMatch(labeler, matcher), updInputs)

            val predExpr: RelExpr = updPredOpt match {
                case Some(expr) => RelOpExpr(Select(expr), List(matchExpr))
                case None => matchExpr
            }

            Some(RelOpExpr(Project(updTargets), List(predExpr)))

        case RelOpExpr(op@Aggregate(targets, groupExprs, predOpt), inputs, None)
        if !op.isComplexGroupBy &&
           targets.exists { t => t.isNestedAggregate } =>
            val (updTargets, targetAggrMap) = extractAggregates(targets)

            val (updPredOpt, aggrMap) = predOpt match {
                case Some(pred) =>
                    val (updPred, predAggrMap) =
                        extractAggregates(pred, targetAggrMap)
                    (Some(updPred), predAggrMap)
                case None => (None, targetAggrMap)
            }

            val inputCols: List[RenameCol] = {
                targets.flatMap { t =>
                    t.expr.scalarCols.map { col => RenameCol(col, col) }
                } :::
                predOpt.toList.flatMap { expr =>
                    expr.scalarCols.map { col => RenameCol(col, col) }
                }
            } distinct

            val aggrTargets: List[ScalarTarget] =
                aggrMap.toList.map { case ((op, inps), alias) =>
                    AliasedExpr(ScalOpExpr(op, inps), alias)
                }

            val aggregate: Aggregate =
                Aggregate(inputCols:::aggrTargets, groupExprs, None)
            val aggregateExpr: RelOpExpr = RelOpExpr(aggregate, inputs)

            val predExpr: RelExpr = updPredOpt match {
                case Some(expr) => RelOpExpr(Select(expr), List(aggregateExpr))
                case None => aggregateExpr
            }

            Some(RelOpExpr(Project(updTargets), List(predExpr)))

        case _ => None
    }

    private def conjunctsPred(conjuncts: List[ScalExpr]): ScalExpr = {
        val initPred: ScalExpr = BoolConst(true)
        conjuncts.distinct.foldLeft (initPred) {
            case (fpred@BoolConst(false), _) => fpred
            case (_, fpred@BoolConst(false)) => fpred
            case (BoolConst(true), pred) => pred
            case (prevPred, BoolConst(true)) => prevPred
            case (prevPred, pred) => ScalOpExpr(And, List(prevPred, pred))
        }
    }

    private def predConjuncts(pred: ScalExpr): List[ScalExpr] = pred match {
        case ScalOpExpr(And, inputs) =>
            inputs.flatMap { c => predConjuncts(c) }
        case _ => List(pred)
    }

    // TODO: Replace with scalExpr.colRefs
    private def colRefs(scalExpr: ScalExpr): List[ColRef] = scalExpr match {
        case (colRef: ColRef) => List(colRef)

        case ScalOpExpr(_, inputs) => inputs.flatMap { c => colRefs(c) }

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            val argColRefs: List[ColRef] = colRefs(argExpr)

            val whenThenColRefs: List[ColRef] = whenThen.flatMap {
                case (w, t) => colRefs(w):::colRefs(t)
            }

            val defaultColRefs: List[ColRef] = colRefs(defaultExpr)

            argColRefs:::whenThenColRefs:::defaultColRefs

        case (annot: AnnotColRef) =>
            throw new RuntimeException(
                "Found unexpected annotated colref: " + annot
            )

        case _ => Nil
    }

    private def extractAggregates(
        targets: List[ScalarTarget],
        aggrMap: Map[(AggregateFunction, List[ScalExpr]), ColRef] = Map()
    ):
    (List[ScalarTarget], Map[(AggregateFunction, List[ScalExpr]), ColRef]) = {
        val (reversed, updAggrMap) =
            targets.foldLeft ((List[ScalarTarget](), aggrMap)) {
                case ((prevTargets, prevAggrMap), target) =>
                    val (nextTarget, nextAggrMap) =
                        extractAggregates(target, prevAggrMap)
                    (nextTarget::prevTargets, nextAggrMap)
            }

        (reversed.reverse, updAggrMap)
    }

    private def extractAggregates(
        target: ScalarTarget,
        aggrMap: Map[(AggregateFunction, List[ScalExpr]), ColRef]
    ): (ScalarTarget, Map[(AggregateFunction, List[ScalExpr]), ColRef]) =
        target match {
            case AliasedExpr(expr, alias) =>
                val (nextExpr, nextAggrMap) = extractAggregates(expr, aggrMap)
                (ScalarTarget(nextExpr, alias), nextAggrMap)
            case other => (other, aggrMap)
        }

    private def extractAggregates(
        expr: ScalExpr,
        aggrMap: Map[(AggregateFunction, List[ScalExpr]), ColRef]
    ):
    (ScalExpr, Map[(AggregateFunction, List[ScalExpr]), ColRef]) = expr match {
        case ScalOpExpr(op@AggregateFunction(_, FuncAll), inputs) =>
            aggrMap.get((op, inputs)) match {
                case Some(alias) =>
                    (alias, aggrMap)
                case None =>
                    val alias: ColRef = ColRef(Counter.nextSymbol("F"))
                    (alias, aggrMap + ((op, inputs) -> alias))
            }

        case ScalOpExpr(op, inputs) =>
            val (reversed, updAggrMap) =
                inputs.foldLeft ((List[ScalExpr](), aggrMap)) {
                    case ((prev, prevAggrMap), inp) =>
                        val (updInp, nextAggrMap) =
                            extractAggregates(inp, prevAggrMap)

                        (updInp::prev, nextAggrMap)
                }

            (ScalOpExpr(op, reversed.reverse), updAggrMap)

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            val (updArgExpr, argExprAggrMap) =
                extractAggregates(argExpr, aggrMap)

            val (reversed, whenThenAggrMap) =
                whenThen.foldLeft (
                    (List[(ScalExpr, ScalExpr)](), argExprAggrMap)
                ) { case ((prev, prevAggrMap), (w, t)) =>
                    val (updw, wAggrMap) = extractAggregates(w, prevAggrMap)
                    val (updt, tAggrMap) = extractAggregates(t, wAggrMap)

                    ((updw, updt)::prev, tAggrMap)
                }

            val (updDefaultExpr, updAggrMap) =
                extractAggregates(defaultExpr, whenThenAggrMap)

            (CaseExpr(updArgExpr, reversed.reverse, updDefaultExpr), updAggrMap)

        case other => (other, aggrMap)
    }

    private def nodisplay(
        name: String,
        rule: RelExpr => Option[RelExpr]
    )(before: RelExpr): Option[RelExpr] = rule(before)

    private def display(
        name: String,
        rule: RelExpr => Option[RelExpr]
    )(before: RelExpr): Option[RelExpr] = {
        val ret: Option[RelExpr] = rule(before)

        if( !Location.isSystemExpr(before) ) {
            ret.foreach { after =>
                println
                println("Before: " + name)
                println(before)

                println
                println("After: " + name)
                println(after)
            }
        }

        ret
    }
}
