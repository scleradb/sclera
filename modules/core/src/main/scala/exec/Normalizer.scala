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
import scala.collection.MapView

import com.scleradb.dbms.location.{Location, LocationId}

import com.scleradb.sql.types._
import com.scleradb.sql.objects._
import com.scleradb.sql.expr._
import com.scleradb.sql.statements._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.exec.{ScalCastEvaluator, ScalExprEvaluator}

import com.scleradb.external.expr.ExternalSourceExpr

import com.scleradb.analytics.transform.expr.Transform
import com.scleradb.analytics.infertypes.expr.InferTypes

import com.scleradb.analytics.nlp.expr.NlpRelOp

import com.scleradb.analytics.sequence.labeler._
import com.scleradb.analytics.sequence.matcher._
import com.scleradb.analytics.sequence.matcher.aggregate._
import com.scleradb.analytics.sequence.matcher.expr.{Match, LabeledMatch}

import com.scleradb.analytics.ml.imputer.expr.Impute
import com.scleradb.analytics.ml.imputer.datatypes.ImputeSpec

import com.scleradb.analytics.ml.classifier.expr.Classify
import com.scleradb.analytics.ml.clusterer.expr.Cluster

import com.scleradb.util.automata.datatypes.Label
import com.scleradb.util.automata.nfa.AnchoredNfa

import com.scleradb.util.tools.Counter

/** Normalizes statements before evaluation */
class Normalizer(
    schema: Schema,
    scalExprEvaluator: ScalExprEvaluator
) {
    private val isSeqEnabled: Boolean = true

    def normalizeStatement(stmt: SqlStatement): SqlStatement = stmt match {
        case (qstmt: SqlRelQueryStatement) => normalizeQueryStatement(qstmt)
        case (ustmt: SqlUpdateStatement) => normalizeUpdateStatement(ustmt)
        case (astmt: SqlAdminStatement) => astmt
        case _ =>
            throw new RuntimeException("Cannot normalize statement: " + stmt)
    }

    def normalizeQueryStatement(
        qstmt: SqlRelQueryStatement
    ): SqlRelQueryStatement = qstmt match {
        case SqlRelQueryStatement(expr) =>
            SqlRelQueryStatement(normalizeRelExpr(expr))
        case _ =>
            throw new RuntimeException("Cannot normalize statement: " + qstmt)
    }

    def normalizeUpdateStatement(
        ustmt: SqlUpdateStatement
    ): SqlUpdateStatement = ustmt match {
        case (usstmt: SqlUpdateSchema) => normalizeUpdateSchema(usstmt)
        case (utstmt: SqlUpdateTable) => normalizeUpdateTable(utstmt)
        case (uistmt: SqlIndex) => normalizeIndexStatement(uistmt)
        case SqlUpdateBatch(stmts) =>
            SqlUpdateBatch(stmts.map { stmt => normalizeUpdateTable(stmt) })
        case (native: SqlNativeStatement) => native
        case _ =>
            throw new RuntimeException("Cannot normalize statement: " + ustmt)
    }

    private def normalizeUpdateSchema(
        usstmt: SqlUpdateSchema
    ): SqlUpdateSchema = usstmt match {
        case SqlCreateExt(dataTarget, relExpr) =>
            SqlCreateExt(dataTarget, normalizeRelExpr(relExpr))
        case SqlCreateDbObject(obj, dur) =>
            SqlCreateDbObject(normalizeObject(obj), dur)
        case SqlCreateMLObject(libOpt, obj, relExpr, dur) =>
            SqlCreateMLObject(libOpt, obj, normalizeRelExpr(relExpr), dur)
        case (drop: SqlDrop) => drop
        case _ =>
            throw new RuntimeException("Cannot normalize statement: " + usstmt)
    }

    private def normalizeUpdateTable(
        utstmt: SqlUpdateTable
    ): SqlUpdateTable = utstmt match {
        case SqlInsertValueRows(tableId, targetCols, rows) =>
            val tableRef: TableRefTarget =
                TableRefTargetById(schema, tableId, targetCols)
            val updRows: List[Row] = rows.map { case Row(scalars) =>
                val updScalars: List[ScalColValue] =
                    tableRef.tableColRefs.zip(scalars).map { case (colRef, v) =>
                        val col: Column =
                            tableRef.table.column(colRef) getOrElse {
                                throw new IllegalArgumentException(
                                    "Column \"" + colRef.repr + "\" not found"
                                )
                            }

                        ScalCastEvaluator.castScalColValue(v, col.sqlType)
                    }

                Row(updScalars)
            }

            SqlInsertValueRows(tableId, targetCols, updRows)

        case SqlInsertQueryResult(tableId, targetCols, Values(_, rows)) =>
            normalizeUpdateTable(SqlInsertValueRows(tableId, targetCols, rows))

        case SqlInsertQueryResult(tableId, targetCols, query) =>
            SqlInsertQueryResult(
                tableId, targetCols,
                normalizeRelExpr(query, Some(tableId.locationId))
            )

        case SqlUpdate(tableId, colValPairs, pred) =>
            val tableRef: TableRefSource = TableRefSourceById(schema, tableId)
            val colMap: RelColMap = relObjectColMap(tableRef)
            SqlUpdate(tableId, colValPairs, normalizeScalExpr(pred, colMap))

        case SqlDelete(tableId, pred) =>
            val tableRef: TableRefSource = TableRefSourceById(schema, tableId)
            val colMap: RelColMap = relObjectColMap(tableRef)
            SqlDelete(tableId, normalizeScalExpr(pred, colMap))

        case _ =>
            throw new RuntimeException("Cannot normalize statement: " + utstmt)
    }

    private def normalizeIndexStatement(
        uistmt: SqlIndex
    ): SqlIndex = uistmt match {
        case SqlCreateIndex(name, relationId, indexCols, pred) =>
            val objRef: RelRefSource = RelExpr.relRefSource(schema, relationId)
            val colMap: RelColMap = relObjectColMap(objRef)
            SqlCreateIndex(
                name, relationId, indexCols, normalizeScalExpr(pred, colMap)
            )

        case dropIndex@SqlDropIndex(_) => dropIndex

        case _ =>
            throw new RuntimeException("Cannot normalize statement: " + uistmt)
    }

    private def relObjectColMap(
        relObjectRef: RelRefSource
    ): RelColMap = RelColMap(
        relObjectRef.tableColRefs.map { col =>
            ColRewriteSpec(List(relObjectRef.name), col.name, col)
        }
    )

    private def normalizeObject(obj: SqlDbObject): SqlDbObject = obj match {
        case sqlTable@SqlTable(_, _, None) => sqlTable

        case SqlTable(table, locIdOpt, Some(relExpr)) =>
            val baseRelExpr: RelExpr = normalizeRelExpr(relExpr, locIdOpt)
            if( baseRelExpr.tableColRefs.size != table.columns.size ) {
                throw new IllegalArgumentException(
                    "The table definition is not compatible" +
                    " with the query result (different number of columns)"
                )
            }

            SqlTable(table, locIdOpt, Some(baseRelExpr))

        case SqlObjectAsExpr(name, objExpr, objectStatus) =>
            SqlObjectAsExpr(name, normalizeRelExpr(objExpr), objectStatus)

        case _ => throw new RuntimeException("Cannot normalize object: " + obj)
    }

    def normalizeRelExpr(
        relExpr: RelExpr,
        targetLocationIdOpt: Option[LocationId] = None
    ): RelExpr = {
        val viewExpandedRelExpr: RelExpr = expandViews(relExpr)

        val (rootNormalized, rootColMap) =
            normalizeRelExprHelper(viewExpandedRelExpr)

        val renameColMap: Map[ColRef, ColRef] = Map() ++
            rootColMap.specs.map { spec =>
                val labeledColRef: LabeledColRef =
                    LabeledColRef(spec.labels, None, spec.name)
                spec.alias -> ColRef(labeledColRef.defaultAlias)
            }

        val renameCols: List[RenameCol] =
            rootNormalized.tableColRefs.flatMap { col =>
                renameColMap.get(col).map { renameCol =>
                    RenameCol(col, renameCol)
                }
            }

        val uniqueRenameCols: List[RenameCol] =
            renameCols.foldLeft (List[RenameCol]()) {
                case (prev, RenameCol(col, alias)) =>
                    val usedAliases: List[ColRef] =
                        prev.map { case RenameCol(_, a) => a }
                    val nextAliases: List[ColRef] =
                        renameCols.drop(prev.size + 1).map {
                            case RenameCol(_, a) => a
                        }
                    val takenAliases: List[ColRef] =
                        usedAliases:::(nextAliases.filter { a => a != alias })

                    RenameCol(col, uniqueAlias(takenAliases, alias))::prev
            }

        // println
        // println(
        //     "[RootNormalized]" + rootNormalized + " -> " +
        //     rootNormalized.locationIdOpt
        // )

        // rename after evaluation so that the sort order is not lost
        val normalizedInp: RelExpr =
            if( rootNormalized.locationIdOpt == targetLocationIdOpt )
                rootNormalized
            else RelOpExpr(EvaluateOp, List(rootNormalized))

        val normalizedRelExpr: RelExpr =
            RelOpExpr(Project(uniqueRenameCols.reverse), List(normalizedInp))

        //  println
        //  println(
        //      "[Normalized]" + normalizedRelExpr + " -> " +
        //      normalizedRelExpr.resultOrder
        //  )

        val seqAugmentedRelExpr: RelExpr =
            if( isSeqEnabled ) augmentSequenceOps(normalizedRelExpr, Map())
            else normalizedRelExpr

        //   println
        //  println(
        //      "[Augmented]" + seqAugmentedRelExpr + " -> " +
        //      seqAugmentedRelExpr.resultOrder
        //  )

        val locAssignedRelExpr: RelExpr =
            assignEvalLocation(seqAugmentedRelExpr)

        // if( !Location.isSystemExpr(relExpr) ) {
        //     println
        //     println("[Assigned]" + locAssignedRelExpr + " -> " +
        //             locAssignedRelExpr.resultOrder)
        // }

        // need to separate optimization and normalization
        val optimizedRelExpr: RelExpr =
            Optimizer.optimizeRelExpr(locAssignedRelExpr)

        // if( !Location.isSystemExpr(relExpr) ) {
        //     println
        //     println("[Optimized]" + optimizedRelExpr + " -> " +
        //             optimizedRelExpr.resultOrder)
        // }

        optimizedRelExpr
    }

    private def uniqueAlias(
        takenAliases: List[ColRef],
        alias: ColRef,
        n: Int = 0
    ): ColRef = {
        val tryAlias: ColRef =
            if( n == 0 ) alias else ColRef(alias.name + "_" + n)

        if( takenAliases.exists { a => a == tryAlias } )
            uniqueAlias(takenAliases, alias, n + 1)
        else tryAlias
    }

    private def expandViews(
        relExpr: RelExpr
    ): RelExpr = relExpr match {
        case (viewRef: ViewRef) =>
            val rewrite: RelExpr =
                RelOpExpr(TableAlias(viewRef.name, viewRef.tableColRefs),
                          List(viewRef.view.expr))
            expandViews(rewrite)

        case RelOpExpr(op, inputs, locIdOpt) =>
            val inpsExpanded: List[RelExpr] =
                inputs.map { inp => expandViews(inp) }
            RelOpExpr(op, inpsExpanded, locIdOpt)

        case other => other
    }

    private def seqSpecRelOp(
        inputNameOpt: Option[String],
        input: RelExpr,
        partnCols: List[ColRef],
        seqSpec: Map[FunctionSpec, ColRef]
    ): RelExpr = {
        val renameList: List[(ColRef, ColRef)] = seqSpec.toList.flatMap {
            case (LabeledColSpec(_, None, name), alias) =>
                Some(ColRef(name) -> alias)
            case _ => None
        }

        if( renameList.size == seqSpec.size ) {
            // no index
            val renameMap: MapView[ColRef, List[ColRef]] =
                renameList.groupBy(_._1).view.mapValues { as => as.map(_._2) }

            val renames: List[RenameCol] = input.tableColRefs.flatMap { col =>
                renameMap.get(col) match {
                    case Some(as) => as.map { alias => RenameCol(col, alias) }
                    case None => Nil // unused column, projected out
                }
            }

            // ensure that the result order is retained
            val updInput: RelExpr =
                if( input.resultOrder.isEmpty || input.locationIdOpt.isEmpty )
                    input
                else RelOpExpr(EvaluateOp, List(input))
            
            RelOpExpr(Project(renames), List(updInput))
        } else {
            val inputName: String = inputNameOpt getOrElse {
                throw new IllegalArgumentException(
                    "Unable to process sequence operations"
                )
            }

            val label: Label = Label(inputName)
            val labeler: RowLabeler = ConstRowLabeler(label)
            val nfa: AnchoredNfa = AnchoredNfa.kleenePlus(label, true, false)
            seqSpecMatchOp(input, partnCols, seqSpec, labeler, nfa)
        }
    }

    private def seqSpecMatchOp(
        input: RelExpr,
        partnCols: List[ColRef],
        seqSpec: Map[FunctionSpec, ColRef],
        labeler: RowLabeler,
        nfa: AnchoredNfa
    ): RelExpr = {
        val (retainedColRenames, aggrColSpecs, paramAliasMap) =
            seqSpec.toList.foldLeft (
                (
                    List[RenameCol](),
                    List[SeqAggregateColSpec](),
                    Map[ScalExpr, ColRef]()
                )
            ) {
                case ((prevRenames, prevAggrSpecs, prevParamAliasMap),
                      (LabeledColSpec(Nil, None, colName), resCol)) =>
                    val col: ColRef = ColRef(colName)
                    val rename: RenameCol = RenameCol(col, resCol)

                    val nextParamAliasMap: Map[ScalExpr, ColRef] =
                        prevParamAliasMap + (col -> col)
                    (rename::prevRenames, prevAggrSpecs, nextParamAliasMap)

                case ((prevRenames, prevAggrSpecs, prevParamAliasMap),
                      (LabeledColSpec(labels, None, colName), resCol))
                if labeler.isRedundant(labels.map { id => Label(id) }) =>
                    val col: ColRef = ColRef(colName)
                    val rename: RenameCol = RenameCol(col, resCol)

                    val nextParamAliasMap: Map[ScalExpr, ColRef] =
                        prevParamAliasMap + (col -> col)
                    (rename::prevRenames, prevAggrSpecs, nextParamAliasMap)

                case ((prevRenames, prevAggrSpecs, prevParamAliasMap),
                      (LabeledColSpec(labels, indexOpt, colName), resCol)) =>
                    val col: ColRef = ColRef(colName)
                    val colSpec: SeqColumnSpec =
                        SeqColumnSpec(
                            resCol, col, indexOpt,
                            labels.map { id => Label(id) }
                        )

                    val nextParamAliasMap: Map[ScalExpr, ColRef] =
                        prevParamAliasMap + (col -> col)
                    (prevRenames, colSpec::prevAggrSpecs, nextParamAliasMap)

                case ((prevRenames, prevAggrSpecs, prevParamAliasMap),
                    (AggregateSpec(labels, funcName, params), resCol)) =>
                    val (nextParamAliasMap, updParams) =
                        params.reverse.foldLeft (
                            (prevParamAliasMap, List[ScalBaseExpr]())
                        ) {
                            case ((aliasMap, ps), v: ScalColValue) =>
                                (aliasMap, v::ps)
                            case ((aliasMap, ps), col: ColRef) =>
                                (aliasMap + (col -> col), col::ps)
                            case ((aliasMap, ps), expr) =>
                                aliasMap.get(expr) match {
                                    case Some(alias) => (aliasMap, alias::ps)
                                    case None =>
                                        val alias: ColRef =
                                            ColRef(Counter.nextSymbol("R"))
                                        (aliasMap + (expr -> alias), alias::ps)
                                }
                        }

                    val funcSpec: SeqFunctionSpec =
                        SeqFunctionSpec(
                            resCol, funcName, updParams,
                            labels.map { id => Label(id) }
                        )

                    (prevRenames, funcSpec::prevAggrSpecs, nextParamAliasMap)
            }

        // reorder the renames in the input column order
        val retainedColRenameMap: Map[ColRef, List[RenameCol]] = Map() ++
            retainedColRenames.groupBy { case RenameCol(col, _) => col }

        val reorderedRetainedColRenames: List[RenameCol] =
            input.tableColRefs.flatMap { col =>
                retainedColRenameMap.get(col) getOrElse Nil
            }

        val aggrRenames: List[RenameCol] =
            aggrColSpecs.map { spec =>
                RenameCol(spec.resultColRef, spec.resultColRef)
            }

        val renames: List[RenameCol] = reorderedRetainedColRenames:::aggrRenames

        val retainedCols: List[ColRef] =
            reorderedRetainedColRenames.map {
                case RenameCol(col, _) => col
            } distinct

        val matcher: RowSequenceMatcher =
            RowSequenceMatcher(
                nfa,
                SeqAggregateColSetSpec(aggrColSpecs, retainedCols),
                partnCols
            )

        val paramTargets: List[ScalarTarget] =
            paramAliasMap.toList.map {
                case (v: ScalColValue, alias) => ValueCol(v, alias)
                case (col: ColRef, alias) => RenameCol(col, alias)
                case (expr, alias) => AliasedExpr(expr, alias)
            }

        val partnTargets: List[RenameCol] =
            partnCols.map { col => RenameCol(col, col) }

        val (sortTargets, sortExprs) = input.resultOrder.map {
            case SortExpr(v: ScalColValue, sDir, nOrder) =>
                val a: ColRef = ColRef(Counter.nextSymbol("Z"))
                (ValueCol(v, a), SortExpr(a, sDir, nOrder))
            case sortExpr@SortExpr(col: ColRef, _, _) =>
                (RenameCol(col, col), sortExpr)
            case SortExpr(expr, sDir, nOrder) =>
                val a: ColRef = ColRef(Counter.nextSymbol("Z"))
                (AliasedExpr(expr, a), SortExpr(a, sDir, nOrder))
        } unzip

        val labelerTargets: List[RenameCol] =
            labeler.requiredCols.map { col => RenameCol(col, col) }

        val inpTargets: List[ScalarTarget] =
            (paramTargets:::partnTargets:::sortTargets:::labelerTargets) match {
                case Nil =>
                    List(ValueCol(SqlNull(), ColRef(Counter.nextSymbol("X"))))
                case nonEmpty => nonEmpty.distinct
            }

        val updInput: RelExpr =
            RelOpExpr(Project(inpTargets), List(input))
        val sortedInput: RelExpr = sortExprs match {
            case Nil => updInput
            case nonEmpty => RelOpExpr(Order(nonEmpty), List(updInput))
        }

        val matchExpr: RelExpr =
            RelOpExpr(LabeledMatch(labeler, matcher), List(sortedInput))

        RelOpExpr(Project(renames), List(matchExpr))
    }

    private def augmentSequenceOps(
        relExpr: RelExpr,
        seqSpec: Map[FunctionSpec, ColRef]
    ): RelExpr = relExpr match {
        case (baseExpr: RelBaseExpr) =>
            if( seqSpec.isEmpty ) baseExpr
            else seqSpecRelOp(Some(baseExpr.name), baseExpr, Nil, seqSpec)

        case RelOpExpr(op@TableAlias(name, _, partnCols), List(input),
                       locIdOpt) =>
            val aggrInpSeqSpec: Map[FunctionSpec, ColRef] =
                inputSeqSpec(seqSpec)

            val (updPartnCols, updSeqSpec) =
                rewriteColsFuncSpecs(partnCols, aggrInpSeqSpec)

            val updInput: RelExpr = augmentSequenceOps(input, updSeqSpec)
            val updAliasExpr: RelExpr = RelOpExpr(op, List(updInput), locIdOpt)

            if( seqSpec.isEmpty ) updAliasExpr
            else seqSpecRelOp(Some(name), updAliasExpr, partnCols, seqSpec)

        case RelOpExpr(Match(regex, labelerOpt, partnCols), List(input), _) =>
            val aggrInpSeqSpec: Map[FunctionSpec, ColRef] =
                inputSeqSpec(seqSpec)

            val (updLabelerOpt, updLabelerOptSeqSpec) = labelerOpt match {
                case Some(labeler) =>
                    val (updLabeler, updLabelerSeqSpec) =
                        rewriteLabelerFuncSpecs(labeler, aggrInpSeqSpec)
                    (Some(updLabeler), updLabelerSeqSpec)
                case None => (None, aggrInpSeqSpec)
            }

            val (updPartnCols, updSeqSpec) =
                rewriteColsFuncSpecs(partnCols, updLabelerOptSeqSpec)

            val updMatchOp: Match = Match(regex, updLabelerOpt, updPartnCols)
            val updInput: RelExpr = augmentSequenceOps(input, updSeqSpec)

            if( seqSpec.isEmpty ) updInput
            else seqSpecMatchOp(
                updInput, updMatchOp.partitionCols, seqSpec,
                updMatchOp.labeler, updMatchOp.anchoredNfa
            )

        case RelOpExpr(LabeledMatch(labeler, matcher), List(input), _) =>
            val initSeqSpec: Map[FunctionSpec, ColRef] =
                if( matcher.isArgMatch ) seqSpec else Map()
            val (updLabeler, updLabelerSeqSpec) =
                rewriteLabelerFuncSpecs(labeler, initSeqSpec)

            val (updMatcher, updSeqSpec) =
                rewriteMatcherFuncSpecs(matcher, updLabelerSeqSpec)

            val updMatchOp: LabeledMatch = LabeledMatch(updLabeler, updMatcher)

            val updInput: RelExpr = augmentSequenceOps(input, updSeqSpec)
            val updMatchExpr: RelExpr = RelOpExpr(updMatchOp, List(updInput))

            if( seqSpec.isEmpty ) updMatchExpr
            else seqSpecRelOp(None, updMatchExpr, Nil, seqSpec)

        case RelOpExpr(projOp: ProjectBase, List(input), locIdOpt) =>
            val (updProjOp, updSeqSpec) = rewriteRelOpFuncSpecs(projOp, Map())

            val updInput: RelExpr = augmentSequenceOps(input, updSeqSpec)
            val updProjExpr: RelExpr =
                RelOpExpr(updProjOp, List(updInput), locIdOpt)

            if( seqSpec.isEmpty ) updProjExpr
            else seqSpecRelOp(None, updProjExpr, Nil, seqSpec)

        case RelOpExpr(op, inputs, locIdOpt) =>
            val (updOp, updSeqSpec) = rewriteRelOpFuncSpecs(op, seqSpec)

            val pushDown: List[(RelExpr, Map[FunctionSpec, ColRef])] =
                inputs.map { inp =>
                    val inpSeqSpec: Map[FunctionSpec, ColRef] =
                        updSeqSpec.view.filterKeys { spec =>
                            spec.cols.diff(inp.tableColRefs).isEmpty
                        } toMap

                    (inp, inpSeqSpec)
                }

            val remSpecs: List[FunctionSpec] = updSeqSpec.keys.toList diff {
                pushDown.flatMap { case (_, inpSeqSpec) => inpSeqSpec.keys }
            }

            if( !remSpecs.isEmpty ) {
                val spec: FunctionSpec = remSpecs.head
                if( spec.labels.isEmpty ) {
                    throw new IllegalArgumentException(
                        "Columns " +
                        spec.cols.map(
                            col => "\"" + col.repr + "\""
                        ).mkString(", ") + " not found"
                    )
                } else {
                    throw new IllegalArgumentException(
                        "Columns " + spec.cols.map(
                            col => "\"" + col.repr + "\""
                        ).mkString(", ") +
                        " not found in tables " + spec.labels.map(
                            label => "\"" + label + "\""
                        ).mkString(", ")
                    )
                }
            }

            val inpsAugmented: List[RelExpr] =
                pushDown.map { case (inp, inpSeqSpec) =>
                    augmentSequenceOps(inp, inpSeqSpec)
                }

            RelOpExpr(updOp, inpsAugmented, locIdOpt)

        case other => other
    }

    private def inputSeqSpec(
        seqSpec: Map[FunctionSpec, ColRef]
    ): Map[FunctionSpec, ColRef] = {
        val cols: List[ColRef] =
            seqSpec.keys.toList.flatMap { spec => spec.cols }

        Map() ++ cols.distinct.map { col =>
            (LabeledColSpec(Nil, None, col.name) -> col)
        }
    }

    private def rewriteRelOpFuncSpecs(
        op: RelOp,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (RelOp, Map[FunctionSpec, ColRef]) = op match {
        case Project(targetExprs) =>
            val (updTargetExprs, updSeqSpec) =
                rewriteTargetExprsFuncSpecs(targetExprs, seqSpec)

            (Project(updTargetExprs), updSeqSpec)

        case Aggregate(targetExprs, groupExprs, predOpt) =>
            val (updTargetExprs, updSeqSpecTarget) =
                rewriteTargetExprsFuncSpecs(targetExprs, seqSpec)

            val (updGroupExprs, updSeqSpecGroup) =
                rewriteScalExprsFuncSpecs(groupExprs, updSeqSpecTarget)

            val (updPredOpt, updSeqSpec) = predOpt match {
                case Some(pred) =>
                    val (updPred, updSeqSpecPred) =
                        rewriteScalExprFuncSpecs(pred, updSeqSpecGroup)
                    (Some(updPred), updSeqSpecPred)
                case None =>
                    (None, updSeqSpecGroup)
            }

            (Aggregate(updTargetExprs, updGroupExprs, updPredOpt), updSeqSpec)

        case Order(sortExprs) =>
            val (updSortExprs, updSeqSpec) =
                rewriteSortExprsFuncSpecs(sortExprs, seqSpec)

            (Order(updSortExprs), updSeqSpec)

        case DistinctOn(exprs, order) =>
            val (updExprs, updSeqSpecExprs) =
                rewriteScalExprsFuncSpecs(exprs, seqSpec)

            val (updOrder, updSeqSpec) =
                rewriteSortExprsFuncSpecs(order, updSeqSpecExprs)

            (DistinctOn(updExprs, updOrder), updSeqSpec)

        case Select(pred) =>
            val (updPred, updSeqSpec) = rewriteScalExprFuncSpecs(pred, seqSpec)

            (Select(updPred), updSeqSpec)

        case Join(joinType, joinPred: JoinPred) =>
            val (updJoinPred, updSeqSpec) =
                rewriteJoinPredFuncSpecs(joinPred, seqSpec)

            (Join(joinType, updJoinPred), updSeqSpec)

        case Classify(classifierId, labelCol) =>
            val (updLabelCol, updSeqSpec) =
                replaceColRefFuncSpecs(labelCol, seqSpec)

            (Classify(classifierId, updLabelCol), updSeqSpec)

        case Cluster(clustererId, clusterIdCol) =>
            val (updClusterIdCol, updSeqSpec) =
                replaceColRefFuncSpecs(clusterIdCol, seqSpec)

            (Cluster(clustererId, updClusterIdCol), updSeqSpec)

        case Impute(imputeSpecs) =>
            val (updImputeSpecs, updSeqSpec) =
                rewriteImputesFuncSpecs(imputeSpecs, seqSpec)

            (Impute(updImputeSpecs), updSeqSpec)

        case nlpRelOp@NlpRelOp(libOpt, langOpt, name, args, inputCol, _) =>
            val (updArgs, updArgSeqSpec) =
                rewriteScalExprsFuncSpecs(args, seqSpec)
            val (updInputCol, updInputColSeqSpec) =
                rewriteColFuncSpecs(inputCol, updArgSeqSpec)

            val (updResultCols, updSeqSpec) =
                replaceColRefsFuncSpecs(nlpRelOp.resultCols, updInputColSeqSpec)

            val updNlpRelOp: NlpRelOp = NlpRelOp(
                libOpt, langOpt, name, updArgs, updInputCol, updResultCols
            )

            (updNlpRelOp, updSeqSpec)

        case Align(metricExpr, marginOpt) =>
            val (updMetricExpr, updSeqSpec) =
                rewriteScalExprFuncSpecs(metricExpr, seqSpec)

            (Align(updMetricExpr, marginOpt), updSeqSpec)

        case DisjointInterval(
                inpLhsCol, inpRhsCol, outLhsCol, outRhsCol, partnCols
             ) =>
            val (updInpLhsCol, inpLhsSeqSpec) =
                rewriteColFuncSpecs(inpLhsCol, seqSpec)
            val (updInpRhsCol, inpSeqSpec) =
                rewriteColFuncSpecs(inpRhsCol, inpLhsSeqSpec)
            val (updOutLhsCol, outLhsSeqSpec) =
                replaceColRefFuncSpecs(outLhsCol, inpSeqSpec)
            val (updOutRhsCol, outRhsSeqSpec) =
                replaceColRefFuncSpecs(outRhsCol, outLhsSeqSpec)
            val (updPartnCols, updSeqSpec) =
                rewriteColsFuncSpecs(partnCols, outRhsSeqSpec)

            val updDisjointInterval: DisjointInterval =
                DisjointInterval(
                    inpLhsCol, inpRhsCol, outLhsCol, outRhsCol, updPartnCols
                )

            (updDisjointInterval, updSeqSpec)

        case (transOp: Transform) =>
            val (updIn, updInSeqSpec) =
                transOp.in.reverse.foldLeft (
                    (List[(String, ScalExpr)](), seqSpec)
                ) {
                    case ((prev, prevSeqSpec), (name, expr))  =>
                        val (updExpr, updSeqSpec) =
                            rewriteScalExprFuncSpecs(expr, prevSeqSpec)

                        ((name -> updExpr)::prev, updSeqSpec)
                }

            val (updGroup, updGroupSeqSpec) =
                rewriteScalExprsFuncSpecs(transOp.partn, updInSeqSpec)

            val (updOrder, updSeqSpec) =
                rewriteSortExprsFuncSpecs(transOp.order, updGroupSeqSpec)

            val (updOut, updOutSeqSpec) =
                transOp.out.reverse.foldLeft (
                    (List[(String, ColRef)](), updSeqSpec)
                ) {
                    case ((prev, prevSeqSpec), (name, col))  =>
                        val colRef: ColRef = ColRef(col.name)
                        val (updColRef, nextSeqSpec) =
                            replaceColRefFuncSpecs(colRef, prevSeqSpec)

                        ((name -> updColRef)::prev, nextSeqSpec)
                }

            val updTransform: Transform = Transform(
                transOp.transformType, transOp.transformer,
                updIn, updGroup, updOrder, updOut
            )

            (updTransform, updOutSeqSpec)

        case InferTypes(cols, nulls, lookAheadOpt) =>
            val (updCols, updSeqSpec) = rewriteColsFuncSpecs(cols, seqSpec)

            (InferTypes(updCols, nulls, lookAheadOpt), updSeqSpec)

        case UnPivot(outValCol, outKeyCol, inColVals) =>
            val (updOutValCol, outValSeqSpec) =
                replaceColRefFuncSpecs(outValCol, seqSpec)

            val (updOutKeyCol, outKeySeqSpec) =
                replaceColRefFuncSpecs(outKeyCol, outValSeqSpec)

            val (inCols, outVals) = inColVals.unzip

            val (updInCols, updSeqSpec) =
                rewriteColsFuncSpecs(inCols, outKeySeqSpec)

            val updInColVals: List[(ColRef, CharConst)] = updInCols zip outVals

            (UnPivot(updOutValCol, updOutKeyCol, updInColVals), updSeqSpec)

        case OrderedBy(sortExprs) =>
            val (updSortExprs, updSeqSpec) =
                rewriteSortExprsFuncSpecs(sortExprs, seqSpec)

            (OrderedBy(updSortExprs), updSeqSpec)

        case _ => (op, seqSpec)
    }

    private def rewriteImputesFuncSpecs(
        imputeSpecs: List[ImputeSpec],
        seqSpec: Map[FunctionSpec, ColRef]
    ): (List[ImputeSpec], Map[FunctionSpec, ColRef]) =
        rewriteExprsFuncSpecs(rewriteImputeFuncSpecs, imputeSpecs, seqSpec)

    private def rewriteImputeFuncSpecs(
        imputeSpec: ImputeSpec,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (ImputeSpec, Map[FunctionSpec, ColRef]) = {
        val ImputeSpec(imputeCol, flagColOpt, classifierId) = imputeSpec

        val (updImputeCol, updImputeColSeqSpec) =
            rewriteColFuncSpecs(imputeCol, seqSpec)
        val (updFlagColOpt, updSeqSpec) = flagColOpt match {
            case Some(flagCol) =>
                val (updFlagCol, updFlagColSeqSpec) =
                    replaceColRefFuncSpecs(flagCol, updImputeColSeqSpec)
                (Some(updFlagCol), updFlagColSeqSpec)

            case None => (None, updImputeColSeqSpec)
        }

        (ImputeSpec(updImputeCol, updFlagColOpt, classifierId), updSeqSpec)
    }

    private def rewriteLabelerFuncSpecs(
        labeler: RowLabeler,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (RowLabeler, Map[FunctionSpec, ColRef]) = labeler match {
        case (constLabeler: ConstRowLabeler) =>
            (constLabeler, seqSpec)

        case ColumnRowLabeler(colRef, whenThen, elseOpt, wild) =>
            val (updColRef, updSeqSpec) = rewriteColFuncSpecs(colRef, seqSpec)

            (ColumnRowLabeler(updColRef, whenThen, elseOpt, wild), updSeqSpec)

        case (predRowLabeler: PredRowLabeler) =>
            val (preds, labels) = predRowLabeler.predLabels unzip
            val (updPreds, updSeqSpec) =
                rewriteScalExprsFuncSpecs(preds, seqSpec)

            (predRowLabeler.clone(updPreds zip labels), updSeqSpec)

        case _ =>
            throw new RuntimeException("Cannot normalize labeler: " + labeler)
    }

    private def rewriteMatcherFuncSpecs(
        matcher: RowSequenceMatcher,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (RowSequenceMatcher, Map[FunctionSpec, ColRef]) = {
        val (updAggRowsSpec, updAggRowsSeqSpec) =
            rewriteAggregateRowsFuncSpecs(
                matcher.aggregateRowsSpec, seqSpec
            )

        val (updPartnCols, updSeqSpec) =
            rewriteColsFuncSpecs(matcher.partitionCols, updAggRowsSeqSpec)

        (matcher.clone(updAggRowsSpec, updPartnCols), updSeqSpec)
    }

    private def rewriteAggregateRowsFuncSpecs(
        spec: SeqAggregateRowsSpec,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (SeqAggregateRowsSpec, Map[FunctionSpec, ColRef]) = spec match {
        case SeqArgOptsSpec(aggColSpecs) =>
            val (updAggColSpecs, updSeqSpec) =
                rewriteAggregateColsFuncSpecs(aggColSpecs, seqSpec)

            (SeqArgOptsSpec(updAggColSpecs), updSeqSpec)

        case SeqAggregateColSetSpec(aggSpecs, retainedCols) =>
            val (updAggSpecs, updAggSeqSpec) =
                rewriteAggregateColsFuncSpecs(aggSpecs, seqSpec)
            val (updRetainedCols, updSeqSpec) =
                rewriteColsFuncSpecs(retainedCols, updAggSeqSpec)

            (SeqAggregateColSetSpec(updAggSpecs, updRetainedCols), updSeqSpec)
    }

    private def rewriteAggregateColsFuncSpecs(
        aggColSpecs: List[SeqAggregateColSpec],
        seqSpec: Map[FunctionSpec, ColRef]
    ): (List[SeqAggregateColSpec], Map[FunctionSpec, ColRef]) =
        rewriteExprsFuncSpecs(
            rewriteAggregateColFuncSpecs, aggColSpecs, seqSpec
        )

    private def rewriteAggregateColFuncSpecs(
        spec: SeqAggregateColSpec,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (SeqAggregateColSpec, Map[FunctionSpec, ColRef]) = spec match {
        case SeqColumnSpec(resultCol, col, index, labels) =>
            val (updCol, updSeqSpec) = rewriteColFuncSpecs(col, seqSpec)

            (SeqColumnSpec(resultCol, updCol, index, labels), updSeqSpec)

        case SeqFunctionSpec(resultCol, fname, params, labels) =>
            val (updParams, updSeqSpec) =
                rewriteScalExprsFuncSpecs(params, seqSpec)

            (SeqFunctionSpec(resultCol, fname, updParams, labels), updSeqSpec)
    }

    private def rewriteJoinPredFuncSpecs(
        joinPred: JoinPred,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (JoinPred, Map[FunctionSpec, ColRef]) = joinPred match {
        case JoinOn(predExpr) =>
            val (updPredExpr, updSeqSpec) =
                rewriteScalExprFuncSpecs(predExpr, seqSpec)
            (JoinOn(updPredExpr), updSeqSpec)

        case _ =>
            throw new RuntimeException("Found unnormalized JOIN operator")
    }

    private def rewriteExprsFuncSpecs[U, T <: U](
        f: (U, Map[FunctionSpec, ColRef]) => (T, Map[FunctionSpec, ColRef]),
        exprs: List[U],
        seqSpec: Map[FunctionSpec, ColRef]
    ): (List[T], Map[FunctionSpec, ColRef]) = {
        val (reversed, updSeqSpec) = exprs.foldLeft (List[T](), seqSpec) {
            case ((prevExprs, prevSeqSpec), expr) =>
                val (nextExpr, nextSeqSpec) = f(expr, prevSeqSpec)
                (nextExpr::prevExprs, nextSeqSpec)
        }

        (reversed.reverse, updSeqSpec)
    }

    private def rewriteTargetExprsFuncSpecs(
        targetExprs: List[ScalarTarget],
        seqSpec: Map[FunctionSpec, ColRef]
    ): (List[ScalarTarget], Map[FunctionSpec, ColRef]) =
        rewriteExprsFuncSpecs(rewriteTargetExprFuncSpecs, targetExprs, seqSpec)

    private def rewriteTargetExprFuncSpecs(
        targetExpr: ScalarTarget,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (ScalarTarget, Map[FunctionSpec, ColRef]) = targetExpr match {
        case AliasedExpr(expr, alias) =>
            val (updExpr, updSeqSpec) = rewriteScalExprFuncSpecs(expr, seqSpec)
            (AliasedExpr(updExpr, alias), updSeqSpec)

        case RenameCol(col, alias) =>
            val (updCol, updSeqSpec) = rewriteColFuncSpecs(col, seqSpec)
            (RenameCol(updCol, alias), updSeqSpec)

        case valueCol@ValueCol(_, _) => (valueCol, seqSpec)
    }

    private def rewriteSortExprsFuncSpecs(
        sortExprs: List[SortExpr],
        seqSpec: Map[FunctionSpec, ColRef]
    ): (List[SortExpr], Map[FunctionSpec, ColRef]) =
        rewriteExprsFuncSpecs(rewriteSortExprFuncSpecs, sortExprs, seqSpec)

    private def rewriteSortExprFuncSpecs(
        sortExpr: SortExpr,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (SortExpr, Map[FunctionSpec, ColRef]) = sortExpr match {
        case SortExpr(expr, sortDir, nullsOrder) =>
            val (updExpr, updSeqSpec) = rewriteScalExprFuncSpecs(expr, seqSpec)

            (SortExpr(updExpr, sortDir, nullsOrder), updSeqSpec)
    }

    private def rewriteScalExprsFuncSpecs(
        scalExprs: List[ScalExpr],
        seqSpec: Map[FunctionSpec, ColRef]
    ): (List[ScalExpr], Map[FunctionSpec, ColRef]) =
        rewriteExprsFuncSpecs(rewriteScalExprFuncSpecs, scalExprs, seqSpec)

    private def rewriteScalExprFuncSpecs(
        scalExpr: ScalExpr,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (ScalExpr, Map[FunctionSpec, ColRef]) = scalExpr match {
        case (col: ColRefBase) =>
            rewriteColFuncSpecs(col, seqSpec)

        case ScalOpExpr(LabeledFunction(name, labels), inputs) =>
            upsertSpec(
                seqSpec,
                AggregateSpec(labels, name, inputs),
                ColRef(Counter.nextSymbol("A"))
            )

        case ScalOpExpr(op, inputs) =>
            val (updInputs, updSeqSpec) =
                rewriteScalExprsFuncSpecs(inputs, seqSpec)
            (ScalOpExpr(op, updInputs), updSeqSpec)

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            val (updArgExpr, updSeqSpecArg) =
                rewriteScalExprFuncSpecs(argExpr, seqSpec)

            val (updWhenThen, updSeqSpecWhenThen) =
                whenThen.reverse.foldLeft (
                    List[(ScalExpr, ScalExpr)](), updSeqSpecArg
                ) {
                    case ((prev, prevSeqSpec), (w, t)) =>
                        val (updW, updSeqSpecW) =
                            rewriteScalExprFuncSpecs(w, prevSeqSpec)
                        val (updT, updSeqSpecT) =
                            rewriteScalExprFuncSpecs(t, updSeqSpecW)
                        ((updW, updT)::prev, updSeqSpecT)
                }

            val (updDefaultExpr, updSeqSpec) =
                rewriteScalExprFuncSpecs(defaultExpr, updSeqSpecWhenThen)

            (CaseExpr(updArgExpr, updWhenThen, updDefaultExpr), updSeqSpec)

        case other => (other, seqSpec)
    }

    private def rewriteColsFuncSpecs(
        cols: List[ColRefBase],
        seqSpec: Map[FunctionSpec, ColRef]
    ): (List[ColRef], Map[FunctionSpec, ColRef]) =
        rewriteExprsFuncSpecs(rewriteColFuncSpecs, cols, seqSpec)

    private def rewriteColFuncSpecs(
        colRefBase: ColRefBase,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (ColRef, Map[FunctionSpec, ColRef]) = colRefBase match {
        case (col: ColRef) =>
            upsertSpec(seqSpec, LabeledColSpec(Nil, None, col.name), col)

        case AnnotColRef(None, name) =>
            upsertSpec(seqSpec, LabeledColSpec(Nil, None, name), ColRef(name))

        case AnnotColRef(Some(t), name) =>
            upsertSpec(
                seqSpec,
                LabeledColSpec(List(t), None, name),
                ColRef(Counter.nextSymbol("C"))
            )

        case LabeledColRef(Nil, None, name) =>
            upsertSpec(seqSpec, LabeledColSpec(Nil, None, name), ColRef(name))

        case LabeledColRef(labels, indexOpt, name) =>
            upsertSpec(
                seqSpec,
                LabeledColSpec(labels, indexOpt, name),
                ColRef(Counter.nextSymbol("S"))
            )
    }

    private def upsertSpec(
        seqSpec: Map[FunctionSpec, ColRef],
        spec: FunctionSpec,
        col: ColRef
    ): (ColRef, Map[FunctionSpec, ColRef]) = seqSpec.get(spec) match {
        case Some(alias) => (alias, seqSpec)
        case None => (col, seqSpec + (spec -> col))
    }

    private def replaceColRefsFuncSpecs(
        cols: List[ColRef],
        seqSpec: Map[FunctionSpec, ColRef]
    ): (List[ColRef], Map[FunctionSpec, ColRef]) =
        rewriteExprsFuncSpecs(replaceColRefFuncSpecs, cols, seqSpec)

    private def replaceColRefFuncSpecs(
        col: ColRef,
        seqSpec: Map[FunctionSpec, ColRef]
    ): (ColRef, Map[FunctionSpec, ColRef]) = {
        val spec: LabeledColSpec = LabeledColSpec(Nil, None, col.name)
        (seqSpec(spec), seqSpec - spec)
    }

    private def assignEvalLocation(
        relExpr: RelExpr
    ): RelExpr = relExpr match {
        case _ if relExpr.isEvaluable => relExpr

        case RelOpExpr(op, inputs, locIdOpt)
        if !inputs.forall { inp => inp.isEvaluable } =>
            val updInputs: List[RelExpr] =
                inputs.map { input => assignEvalLocation(input) }

            val rewrite: RelExpr = RelOpExpr(op, updInputs, locIdOpt)
            assignEvalLocation(rewrite)

        case RelOpExpr(op, inputs, _)
        if op.isLocEvaluable(Location.dataCacheLocation(inputs.head.schema)) =>
            val rewrite: RelExpr =
                RelOpExpr(op, inputs, Some(Location.dataCacheLocationId))

            assignEvalLocation(rewrite)

        case RelOpExpr(op@Aggregate(_, groupExprs, _), inputs, None) =>
            // cannot be evaluated as a stream, nor in the cache
            // sort input on group-by to enable stream-evaluation
            val sorted: List[SortExpr] =
                inputs.head.resultOrder.takeWhile { case SortExpr(expr, _, _) =>
                    groupExprs contains expr
                }

            val needSort: List[ScalExpr] = groupExprs diff {
                sorted.map { case SortExpr(expr, _, _) => expr }
            }

            if( needSort.isEmpty ) {
                throw new IllegalArgumentException(
                    "Cannot assign a location for computing the aggregate"
                )
            } else {
                val sortExprs: List[SortExpr] = sorted :::
                    needSort.map { expr =>
                        SortExpr(expr, SortAsc, SortAsc.defaultNullsOrder)
                    }

                val rewrite: RelExpr =
                    RelOpExpr(op, List(RelOpExpr(Order(sortExprs), inputs)))

                assignEvalLocation(rewrite)
            }

        case RelOpExpr(Order(sortExprs), inputs, None) =>
            val (sortTargetExprs, updSortExprs) = sortExprs.map {
                case sortExpr@SortExpr(col: ColRef, _, _) =>
                    (RenameCol(col, col), sortExpr)

                case SortExpr(expr, dir, nullsOrd) =>
                    val alias: ColRef = ColRef(Counter.nextSymbol("R"))
                    (ScalarTarget(expr, alias), SortExpr(alias, dir, nullsOrd))
            } unzip

            val targetExprs: List[RenameCol] =
                inputs.head.tableColRefs.map { col => RenameCol(col, col) }
            val ordTargetExprs: List[ScalarTarget] =
                (targetExprs ::: sortTargetExprs).distinct

            val updInput: RelExpr = RelOpExpr(Project(ordTargetExprs), inputs)
            val updOrderExpr: RelExpr =
                RelOpExpr(Order(updSortExprs), List(updInput))
            val rewrite: RelExpr =
                RelOpExpr(Project(targetExprs), List(updOrderExpr))

            assignEvalLocation(rewrite)

        case RelOpExpr(LimitOffset(limOpt, off, sortExprs), inputs, None) =>
            val (sortTargetExprs, updSortExprs) = sortExprs.map {
                case sortExpr@SortExpr(col: ColRef, _, _) =>
                    (RenameCol(col, col), sortExpr)

                case SortExpr(expr, dir, nullsOrd) =>
                    val alias: ColRef = ColRef(Counter.nextSymbol("L"))
                    (ScalarTarget(expr, alias), SortExpr(alias, dir, nullsOrd))
            } unzip

            val targetExprs: List[RenameCol] =
                inputs.head.tableColRefs.map { col => RenameCol(col, col) }
            val ordTargetExprs: List[ScalarTarget] =
                (targetExprs ::: sortTargetExprs).distinct

            val updInput: RelExpr = RelOpExpr(Project(ordTargetExprs), inputs)
            val updLimitExpr: RelExpr =
                  RelOpExpr(LimitOffset(limOpt, off, updSortExprs),
                            List(updInput))
            val rewrite: RelExpr =
                RelOpExpr(Project(targetExprs), List(updLimitExpr))

            assignEvalLocation(rewrite)

        case RelOpExpr(DistinctOn(distinctExprs, sortExprs), inputs, None) =>
            val (distinctTargetExprs, updDistinctExprs) = distinctExprs.map {
                case (col: ColRef) =>
                    (RenameCol(col, col), col)

                case expr =>
                    val alias: ColRef = ColRef(Counter.nextSymbol("D"))
                    (ScalarTarget(expr, alias), alias)
            } unzip

            val (sortTargetExprs, updSortExprs) = sortExprs.map {
                case sortExpr@SortExpr(col: ColRef, _, _) =>
                    (RenameCol(col, col), sortExpr)

                case SortExpr(expr, dir, nullsOrd) =>
                    val alias: ColRef = ColRef(Counter.nextSymbol("D"))
                    (ScalarTarget(expr, alias), SortExpr(alias, dir, nullsOrd))
            } unzip

            val targetExprs: List[RenameCol] =
                inputs.head.tableColRefs.map { col => RenameCol(col, col) }
            val inpTargetExprs: List[ScalarTarget] =
                (targetExprs:::distinctTargetExprs:::sortTargetExprs).distinct

            val updInput: RelExpr = RelOpExpr(Project(inpTargetExprs), inputs)
            val updDistinctExpr: RelExpr =
                  RelOpExpr(DistinctOn(updDistinctExprs, updSortExprs),
                            List(updInput))
            val rewrite: RelExpr =
                RelOpExpr(Project(targetExprs), List(updDistinctExpr))

            assignEvalLocation(rewrite)

        case RelOpExpr(op@DisjointInterval(lhsCol, _, _, _, partnCols),
                       inputs@List(input), None) =>
            val compatSortExprs: List[SortExpr] = input.resultOrder.takeWhile {
                case SortExpr(expr, _, _) => partnCols contains expr
            }

            val compatExprs: List[ScalExpr] = compatSortExprs.map {
                case SortExpr(expr, _, _) => expr
            }

            val reqSortCols: List[ColRef] = partnCols ::: List(lhsCol)
            val remSortCols: List[ColRef] =
                reqSortCols.filter { col => !compatExprs.contains(col) }

            val sortExprs: List[SortExpr] = compatSortExprs :::
                remSortCols.map { col => SortExpr(col, SortAsc, NullsLast) }
            val updInput: RelExpr = RelOpExpr(Order(sortExprs), inputs)
            val rewrite: RelExpr = RelOpExpr(op, List(updInput))

            assignEvalLocation(rewrite)

        case _ =>
            throw new IllegalArgumentException(
                "Cannot assign a location for computing the query"
            )
    }

    private def normalizeRelExprHelper(
        relExpr: RelExpr
    ): (RelExpr, RelColMap) = relExpr match {
        case (values: Values) =>
            normalizeRelExprWrapper(values.name, values)

        case (tableRef: TableRefSource) if tableRef.aliasCols.isEmpty =>
            normalizeRelExprWrapper(tableRef.name, tableRef)

        case (tableRef: TableRefSource) => // non-empty alias list
            val rewrite: RelExpr = RelOpExpr(
                TableAlias(tableRef.name, tableRef.tableColRefs),
                List(TableRefSourceExplicit(schema, tableRef.schemaTable, Nil))
            )

            normalizeRelExprHelper(rewrite)

        case (viewRef: ViewRef) =>
            val rewrite: RelExpr = RelOpExpr(
                TableAlias(viewRef.name, viewRef.tableColRefs),
                List(viewRef.view.expr)
            )
            normalizeRelExprHelper(rewrite)

        case (extExpr: ExternalSourceExpr) =>
            normalizeRelExprWrapper(extExpr.name, extExpr)

        case RelOpExpr(op@Aggregate(targetExprs, groupExprs, predOpt),
                       inputs, locIdOpt)
        if op.isComplexGroupBy =>
            val replExprs: List[ScalExpr] =
                groupExprs :::
                predOpt.toList.flatMap { pred => pred.seqAggregates } :::
                targetExprs.flatMap { t => t.expr.seqAggregates } distinct

            val replExprTargets: List[ScalarTarget] = replExprs.map {
                case (col: ColRef) => RenameCol(col, col)
                case expr =>
                    val alias: ColRef = ColRef(Counter.nextSymbol("G"))
                    AliasedExpr(expr, alias)
            }

            val replExprMap: Map[ScalExpr, ColRef] = Map() ++
                replExprTargets.map { t => t.expr -> t.alias }

            val updGroupExprs: List[ColRef] =
                groupExprs.map { expr => replExprMap(expr) }

            val updTargetExprs: List[ScalarTarget] =
                targetExprs.map { t => t.replace(replExprMap) }

            val updPredOpt: Option[ScalExpr] =
                predOpt.map { pred => pred.replace(replExprMap) }

            val updExprs: List[ScalExpr] =
                updPredOpt.toList ::: updTargetExprs.map { t => t.expr }

            val replAliasList: List[ColRef] =
                replExprTargets.map { t => t.alias }

            val remColsTargets: List[ScalarTarget] = updExprs.flatMap { expr =>
                expr.columns.flatMap {
                    case (col: ColRef) =>
                        if( replAliasList contains col )
                            None // alias added above
                        else Some(RenameCol(col, col))
                    case other =>
                        val alias: ColRef = ColRef(Counter.nextSymbol("X"))
                        Some(ScalarTarget(other, alias))
                }
            }

            val projectTargets: List[ScalarTarget] =
                (replExprTargets ::: remColsTargets).distinct

            val remColsMap: Map[ScalExpr, ColRef] =
                Map() ++ remColsTargets.map { t => t.expr -> t.alias }

            val finalTargetExprs: List[ScalarTarget] =
                updTargetExprs.map { t => t.replace(remColsMap) }

            val finalPredOpt: Option[ScalExpr] =
                updPredOpt.map { pred => pred.replace(remColsMap) }

            val finalExprs: List[ScalExpr] =
                finalPredOpt.toList ::: finalTargetExprs.map { t => t.expr }

            val finalScalarCols: List[ColRef] =
                finalExprs.flatMap { expr => expr.scalarCols } distinct

            val errCols: List[ColRef] = finalScalarCols diff updGroupExprs

            errCols.foreach { errCol =>
                val errTargetOpt: Option[ScalarTarget] =
                    projectTargets.find { t => t.alias == errCol }

                errTargetOpt.foreach { e =>
                    throw new IllegalArgumentException(
                        "Column \"" + e.expr.repr + "\" must appear" +
                        " in the GROUP BY clause or be used in" +
                        " an aggregate function"
                    )
                }
            }

            val rewrite: RelExpr =
                RelOpExpr(
                    Aggregate(finalTargetExprs, updGroupExprs, finalPredOpt),
                    List(RelOpExpr(Project(projectTargets), inputs)),
                    locIdOpt
                )

            normalizeRelExprHelper(rewrite)

        // enable ORDER BY to use expressions from GROUP BY
        // not specified in the SELECT clause
        case RelOpExpr(op: SortRelOp, List(
                 RelOpExpr(Aggregate(targetExprs, groupExprs, predOpt),
                           inputs, None)), None)
        if !groupExprs.filter(e => e.isColRef).diff(
            targetExprs.map { t => t.alias }
        ).isEmpty =>
            val aggrTargetExprs: List[ScalarTarget] =
                groupExprs.diff(targetExprs.map { t => t.alias }).flatMap {
                    case (col: ColRef) => Some(RenameCol(col, col))
                    case _ => None
                } ::: targetExprs

            val aggrExpr: RelExpr =
                 RelOpExpr(Aggregate(aggrTargetExprs, groupExprs, predOpt),
                           inputs)

            val orderExpr: RelExpr = RelOpExpr(op, List(aggrExpr))

            val projTargetExprs: List[RenameCol] =
                targetExprs.map { t => RenameCol(t.alias, t.alias) }

            // evaluation needed to retain sort order
            val rewrite: RelExpr =
                 RelOpExpr(Project(projTargetExprs),
                           List(RelOpExpr(EvaluateOp, List(orderExpr))))

            normalizeRelExprHelper(rewrite)

        case RelOpExpr(op, inputs, locIdOpt) =>
            val inpsNormalized: List[(RelExpr, RelColMap)] =
                inputs.map { inp => normalizeRelExprHelper(inp) }
            normalizeRelOpExprHelper(op, inpsNormalized, locIdOpt)

        case _ => throw new RuntimeException("Cannot normalize: " + relExpr)
    }

    private def normalizeRelExprWrapper(
        name: String,
        relExpr: RelExpr
    ): (RelExpr, RelColMap) = {
        val renames: List[RenameCol] = relExpr.tableColRefs.map { col =>
            RenameCol(col, ColRef(Counter.nextSymbol("B")))
        }

        val wrappedRelExpr: RelExpr =
            RelOpExpr(TableAlias(name),
                      List(RelOpExpr(Project(renames), List(relExpr))))

        val specs: List[ColRewriteSpec] = renames.map {
            case RenameCol(col, alias) =>
                ColRewriteSpec(List(name), col.name, alias)
        }

        (wrappedRelExpr, RelColMap(specs))
    }

    private def normalizeRelOpExprHelper(
        op: RelOp,
        inpsNormalized: List[(RelExpr, RelColMap)],
        locIdOverrideOpt: Option[LocationId]
    ): (RelExpr, RelColMap) = (op, inpsNormalized) match {
        case (matchOp@Match(regexStr, labelerOpt, partnCols),
              List((inpExpr, inpColMap))) =>
            val updLabelerOpt: Option[RowLabeler] = labelerOpt.map { labeler =>
                normalizeLabeler(labeler, inpColMap)
            }

            val updPartnCols: List[ColRef] = partnCols.map { colRef =>
                normalizeColRef(colRef, inpColMap)
            }

            val updMatch: Match = Match(regexStr, updLabelerOpt, updPartnCols)

            // Add labels to the inpColMap
            val updLabels: List[String] =
                matchOp.anchoredNfa.labels.map { label => label.id }

            val outColRewriteSpecs: List[ColRewriteSpec] =
                inpColMap.specs.map { spec =>
                    ColRewriteSpec(updLabels, spec.name, spec.alias, true)
                }

            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)

            (RelOpExpr(updMatch, List(inpExpr)), outColMap)

        case (LabeledMatch(labeler, matcher), List((inpExpr, inpColMap))) =>
            val updLabeler: RowLabeler = normalizeLabeler(labeler, inpColMap)
            val (updMatcher, outColMap) = normalizeMatcher(matcher, inpColMap)

            val updLabeledMatch: LabeledMatch =
                LabeledMatch(updLabeler, updMatcher)

            (RelOpExpr(updLabeledMatch, List(inpExpr)), outColMap)

        case (EvaluateOp, List((inpExpr, inpColMap))) =>
            (RelOpExpr(EvaluateOp, List(inpExpr)), inpColMap)

        case (Align(metricExpr, marginOpt),
              List((lhsInpExpr, lhsInpColMap), (rhsInpExpr, rhsInpColMap))) =>
            val alignColMap: RelColMap =
                RelColMap(lhsInpColMap.specs:::rhsInpColMap.specs)

            val updMetricExpr: ScalExpr =
                normalizeScalExpr(metricExpr, alignColMap)

            val updAlign: Align = Align(updMetricExpr, marginOpt)
            (RelOpExpr(updAlign, List(lhsInpExpr, rhsInpExpr)), alignColMap)

        case (DisjointInterval(inpLhs, inpRhs, outLhs, outRhs, partnCols),
              List((inpExpr, inpColMap))) =>
            val updInpLhs: ColRef = normalizeColRef(inpLhs, inpColMap)
            val updInpRhs: ColRef = normalizeColRef(inpRhs, inpColMap)

            val updOutLhs: ColRef = ColRef(Counter.nextSymbol("D"))
            val updOutRhs: ColRef = ColRef(Counter.nextSymbol("D"))

            val updPartnCols: List[ColRef] =
                partnCols.map { col => normalizeColRef(col, inpColMap) }

            val outColRewriteSpecs: List[ColRewriteSpec] =
                ColRewriteSpec(Nil, outLhs.name, updOutLhs)::
                ColRewriteSpec(Nil, outRhs.name, updOutRhs)::
                inpColMap.specs

            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)
            val updDisjointInterval: DisjointInterval =
                DisjointInterval(
                    updInpLhs, updInpRhs, updOutLhs, updOutRhs, updPartnCols
                )

            (RelOpExpr(updDisjointInterval, List(inpExpr)), outColMap)

        case (transOp: Transform, List((inpExpr, inpColMap))) =>
            val updIn: List[(String, ScalExpr)] =
                transOp.in.map { case (name, e) =>
                    name -> normalizeScalExpr(e, inpColMap)
                }

            val updGroup: List[ScalExpr] =
                transOp.partn.map { e => normalizeScalExpr(e, inpColMap) }

            val updOrder: List[SortExpr] = transOp.order.map {
                case SortExpr(expr, sortDir, nullsOrder) =>
                    SortExpr(normalizeScalExpr(expr, inpColMap),
                             sortDir, nullsOrder)
            }

            val updOut: List[(String, ColRef)] =
                transOp.out.map { case (name, _) =>
                    name -> ColRef(Counter.nextSymbol("U"))
                }

            val replColRef: Map[String, ColRef] = Map() ++ updOut

            val outColRewriteSpecs: List[ColRewriteSpec] =
                transOp.out.map { case (name, col) =>
                    ColRewriteSpec(Nil, col.name, replColRef(name))
                } ::: inpColMap.specs

            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)

            val updTransform: Transform = Transform(
                transOp.transformType, transOp.transformer,
                updIn, updGroup, updOrder, updOut
            )

            (RelOpExpr(updTransform, List(inpExpr)), outColMap)

        case (InferTypes(cols, nulls, lookAheadOpt),
              List((inpExpr, inpColMap))) =>
            val updCols: List[ColRef] =
                cols.map { col => normalizeColRef(col, inpColMap) }

            val updInferTypes: InferTypes =
                InferTypes(updCols, nulls, lookAheadOpt)

            (RelOpExpr(updInferTypes, List(inpExpr)), inpColMap)

        case (UnPivot(outValCol, outKeyCol, inColVals),
              List((inpExpr, inpColMap))) =>
            val updOutValCol: ColRef = ColRef(Counter.nextSymbol("U"))
            val updOutKeyCol: ColRef = ColRef(Counter.nextSymbol("U"))
            val updInColVals: List[(ColRef, CharConst)] =
                inColVals.map { case (col, v) =>
                    (normalizeColRef(col, inpColMap), v)
                }

            val outColRewriteSpecs: List[ColRewriteSpec] =
                ColRewriteSpec(Nil, outValCol.name, updOutValCol)::
                ColRewriteSpec(Nil, outKeyCol.name, updOutKeyCol)::
                inpColMap.specs.filter { spec =>
                    !inColVals.exists { case (col, _) => spec.name == col.name }
                }

            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)
            val updUnPivot: UnPivot =
                UnPivot(updOutValCol, updOutKeyCol, updInColVals)

            (RelOpExpr(updUnPivot, List(inpExpr)), outColMap)

        case (Project(targetExprs), List((inpExpr, inpColMap))) =>
            val (updTargetExprs, colMap) =
                normalizeTargetExprs(targetExprs, inpColMap)

            if( updTargetExprs.isEmpty )
                throw new IllegalArgumentException(
                    "SELECT expression list is empty"
                )

            val updProject: Project = Project(updTargetExprs)
            (RelOpExpr(updProject, List(inpExpr), locIdOverrideOpt), colMap)

        // TODO: Normalize group exprs to eliminate result aliases
        case (Aggregate(targetExprs, groupExprs, predOpt),
              List((inpExpr, inpColMap))) =>
            val (updTargetExprs, outColMap) =
                normalizeTargetExprs(targetExprs, inpColMap)

            if( updTargetExprs.isEmpty ) throw new IllegalArgumentException(
                "Aggregate result expression list is empty"
            )

            val inpColRewriteSpecs: List[ColRewriteSpec] = inpColMap.specs
            val inpColNames: List[String] =
                inpColRewriteSpecs.map { spec => spec.name }

            val updGroupExprs: List[ScalExpr] = groupExprs.map { expr =>
                if( expr.isAggregate ) {
                    throw new IllegalArgumentException(
                        "Aggregate function found in the GROUP BY list"
                    )
                } else normalizeScalExpr(expr, inpColMap)
            }

            val groupCols: List[ColRef] =
                updGroupExprs.flatMap {
                    case (colRef: ColRef) => Some(colRef)
                    case _ => None
                }

            val updTargetCols: List[ColRef] =
                updTargetExprs.flatMap { t => t.expr.scalarCols }

            val updPredOpt: Option[ScalExpr] =
                predOpt.map { pred => normalizeScalExpr(pred, inpColMap) }

            val updPredCols: List[ColRef] =
                updPredOpt.toList.flatMap { pred => pred.scalarCols }

            val errCols: List[ColRef] =
                (updTargetCols:::updPredCols).distinct diff groupCols

            errCols.foreach { colRef =>
                val specOpt: Option[ColRewriteSpec] =
                    inpColMap.specs.find { spec => spec.alias == colRef }
                specOpt.foreach { spec =>
                    val col: LabeledColRef =
                        LabeledColRef(spec.labels, None, spec.name)

                    throw new IllegalArgumentException(
                        "Column \"" + col.repr + "\" must appear" +
                        " in the GROUP BY clause or be used in" +
                        " an aggregate function"
                    )
                }
            }

            val updAggregate: Aggregate =
                Aggregate(updTargetExprs, updGroupExprs, updPredOpt)
            (RelOpExpr(updAggregate, List(inpExpr), locIdOverrideOpt),
             outColMap)

        case (Order(sortExprs), List((inpExpr, inpColMap))) =>
            val updSortExprs: List[SortExpr] = sortExprs.map {
                case SortExpr(expr, sortDir, nullsOrder) =>
                    SortExpr(normalizeScalExpr(expr, inpColMap),
                             sortDir, nullsOrder)
            }

            val updOrder: Order = Order(updSortExprs)
            (RelOpExpr(updOrder, List(inpExpr), locIdOverrideOpt), inpColMap)

        case (LimitOffset(limitOpt, offset, order),
              List((inpExpr, inpColMap))) =>
            val updOrder: List[SortExpr] = order.map {
                case SortExpr(expr, sortDir, nullsOrder) =>
                    SortExpr(
                        normalizeScalExpr(expr, inpColMap), sortDir, nullsOrder
                    )
            }

            val updLimitOffset: LimitOffset =
                LimitOffset(limitOpt, offset, updOrder)
            (RelOpExpr(updLimitOffset, List(inpExpr), locIdOverrideOpt),
             inpColMap)

        case (TableAlias(name, cols, partnCols), List((inpExpr, inpColMap))) =>
            val (toRename, toReassign) = inpColMap.specs.splitAt(cols.length)

            val renamed: List[ColRewriteSpec] =
                toRename.zip(cols).map { case (spec, col) =>
                    ColRewriteSpec(List(name), col.name, spec.alias)
                }

            val reassigned: List[ColRewriteSpec] =
                toReassign.map { spec =>
                    ColRewriteSpec(List(name), spec.name, spec.alias)
                }

            val outColMap: RelColMap = RelColMap(renamed:::reassigned)

            val updPartnCols: List[ColRef] = partnCols.map { colRef =>
                normalizeColRef(colRef, outColMap)
            }

            val updTableAlias: TableAlias = TableAlias(name, Nil, updPartnCols)
            (RelOpExpr(updTableAlias, List(inpExpr), locIdOverrideOpt),
             outColMap)

        case (DistinctOn(exprs, order), List((inpExpr, inpColMap))) =>
            val updExprs: List[ScalExpr] =
                exprs.map { expr => normalizeScalExpr(expr, inpColMap) }

            val updOrder: List[SortExpr] = order.map {
                case SortExpr(expr, sortDir, nullsOrder) =>
                    SortExpr(
                        normalizeScalExpr(expr, inpColMap), sortDir, nullsOrder
                    )
            }

            val updDistinctOn: DistinctOn =
                DistinctOn(updExprs, updOrder)
            (RelOpExpr(updDistinctOn, List(inpExpr), locIdOverrideOpt),
             inpColMap)

        case (Distinct, List((inpExpr, inpColMap))) =>
            (RelOpExpr(Distinct, List(inpExpr), locIdOverrideOpt), inpColMap)

        case (Select(pred), List((inpExpr, inpColMap))) =>
            val updPred: ScalExpr = normalizeScalExpr(pred, inpColMap)

            val updSelect: Select = Select(updPred)
            (RelOpExpr(updSelect, List(inpExpr), locIdOverrideOpt), inpColMap)

        case (Join(joinType, JoinOn(predExpr)),
              List((lhsInpExpr, lhsInpColMap), (rhsInpExpr, rhsInpColMap))) =>
            val joinColMap: RelColMap =
                RelColMap(lhsInpColMap.specs:::rhsInpColMap.specs)

            val updPredExpr: ScalExpr =
                normalizeScalExpr(predExpr, joinColMap) match {
                    case ScalOpExpr(Equals, List(a: ColRef, b: ColRef))
                    if lhsInpExpr.tableColRefs.contains(b) &&
                       rhsInpExpr.tableColRefs.contains(a) =>
                        ScalOpExpr(Equals, List(b: ColRef, a: ColRef))

                    case other => other
                }

            val updJoin: Join = Join(joinType, JoinOn(updPredExpr))
            (RelOpExpr(updJoin, List(lhsInpExpr, rhsInpExpr), locIdOverrideOpt),
             joinColMap)

        case (Join(joinType, JoinUsing(cols)),
              List((lhsInpExpr, lhsInpColMap), (rhsInpExpr, rhsInpColMap))) =>
            val eqPreds: List[ScalExpr] = cols.map { col =>
                ScalOpExpr(
                    Equals, List(
                        normalizeScalExpr(col, lhsInpColMap),
                        normalizeScalExpr(col, rhsInpColMap)
                    )
                )
            }

            val predExpr: ScalExpr = eqPreds.tail.foldLeft (eqPreds.head) {
                case (preds, pred) => ScalOpExpr(And, List(preds, pred))
            }

            val updPred: JoinPred = JoinOn(predExpr)
            val updJoin: Join = Join(joinType, updPred)
            val updJoinExpr: RelExpr =
                RelOpExpr(updJoin, List(lhsInpExpr, rhsInpExpr),
                          locIdOverrideOpt)

            // the excluded columns for JOIN USING excluded from RelColMap
            val colNames: List[String] = cols.map { col => col.name }

            val outColRewriteSpecs: List[ColRewriteSpec] = joinType match {
                case Inner =>
                    val lhsAugmented: List[ColRewriteSpec] =
                        lhsInpColMap.specs.map { lhsSpec =>
                            if( colNames.contains(lhsSpec.name) ) {
                                val rhsSpec: ColRewriteSpec =
                                    rhsInpColMap(lhsSpec.name)
                                ColRewriteSpec(
                                    lhsSpec.labels:::rhsSpec.labels,
                                    lhsSpec.name, lhsSpec.alias
                                )
                            } else lhsSpec
                        }

                    val rhsFiltered: List[ColRewriteSpec] =
                        rhsInpColMap.specs.filter { spec =>
                            !colNames.contains(spec.name)
                        }

                    lhsAugmented:::rhsFiltered

                case (LeftOuter | FullOuter) =>
                    val rhsFiltered: List[ColRewriteSpec] =
                        rhsInpColMap.specs.filter { spec =>
                            !colNames.contains(spec.name)
                        }

                    lhsInpColMap.specs:::rhsFiltered

                case RightOuter =>
                    val lhsFiltered: List[ColRewriteSpec] =
                        lhsInpColMap.specs.filter { spec =>
                            !colNames.contains(spec.name)
                        }

                    lhsFiltered:::rhsInpColMap.specs
            }

            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)

            // wrapper to remove the excluded columns for JOIN USING
            val targetExprs: List[AliasedExpr] =
                outColRewriteSpecs.map { spec =>
                    AliasedExpr(spec.alias, spec.alias)
                }

            (RelOpExpr(Project(targetExprs), List(updJoinExpr),
                       locIdOverrideOpt),
             outColMap)

        case (Join(joinType, JoinNatural),
              List((_, lhsInpColMap), (_, rhsInpColMap))) =>
            // rewrite to JOIN USING
            val lhsCols: List[ColRef] =
                lhsInpColMap.specs.map { spec => ColRef(spec.name) }

            val rhsCols: List[ColRef] =
                rhsInpColMap.specs.map { spec => ColRef(spec.name) }

            val updPred: JoinPred = JoinUsing(lhsCols intersect rhsCols)

            normalizeRelOpExprHelper(
                Join(joinType, updPred), inpsNormalized, locIdOverrideOpt
            )

        case (compoundOp@Compound(_),
              List((lhsInpExpr, lhsInpColMap), (rhsInpExpr, rhsInpColMap))) =>
            if(lhsInpColMap.specs.size != rhsInpColMap.specs.size)
                throw new IllegalArgumentException(
                    "Inputs to a compound operator " +
                    "must have the same number of columns"
                )

            val lhsAliasCols: List[ColRef] =
                lhsInpColMap.specs.map { spec => spec.alias }
            val rhsAliasCols: List[ColRef] =
                rhsInpColMap.specs.map { spec => spec.alias }

            // Make the rhs input schema the same as the lhs input schema
            // by adding a project
            val targetExprs: List[AliasedExpr] =
                lhsAliasCols.zip(rhsAliasCols).map {
                    case (lhsCol, rhsCol) => AliasedExpr(rhsCol, lhsCol)
                }

            val updatedRhsExpr: RelExpr =
                RelOpExpr(Project(targetExprs), List(rhsInpExpr))
            val updatedInpRelExprs: List[RelExpr] =
                List(lhsInpExpr, updatedRhsExpr)

            // schema of the result is that of the lhs input
            (RelOpExpr(compoundOp, updatedInpRelExprs), lhsInpColMap)

        case (classify@Classify(_, labelCol), List((inpExpr, inpColMap))) =>
            // need to add a project below and above the classification
            // because the classifier needs the original feature names
            val inpTargetExprs: List[AliasedExpr] = inpColMap.specs.map {
                spec => AliasedExpr(spec.alias, ColRef(spec.name))
            }

            val updInpExpr: RelExpr =
                RelOpExpr(Project(inpTargetExprs), List(inpExpr))
            val updExpr: RelExpr = RelOpExpr(classify, List(updInpExpr))

            val labelColAlias: ColRef = ColRef(Counter.nextSymbol("A"))
            val labelColRewriteSpec: ColRewriteSpec =
                ColRewriteSpec(Nil, labelCol.name, labelColAlias)

            val outColRewriteSpecs: List[ColRewriteSpec] =
                labelColRewriteSpec::inpColMap.specs
            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)

            val outTargetExprs: List[AliasedExpr] = outColRewriteSpecs.map {
                spec => AliasedExpr(ColRef(spec.name), spec.alias)
            }

            val updOutExpr: RelExpr =
                RelOpExpr(Project(outTargetExprs), List(updExpr))

            (updOutExpr, outColMap)

        case (cluster@Cluster(_, clusterIdCol), List((inpExpr, inpColMap))) =>
            // need to add a project below and above the clustering
            // because the clusterer needs the original feature names
            val inpTargetExprs: List[AliasedExpr] = inpColMap.specs.map {
                spec => AliasedExpr(spec.alias, ColRef(spec.name))
            }

            val updInpExpr: RelExpr =
                RelOpExpr(Project(inpTargetExprs), List(inpExpr))
            val updExpr: RelExpr = RelOpExpr(cluster, List(updInpExpr))

            val idColAlias: ColRef = ColRef(Counter.nextSymbol("U"))
            val idColRewriteSpec: ColRewriteSpec =
                ColRewriteSpec(Nil, clusterIdCol.name, idColAlias)

            val outColRewriteSpecs: List[ColRewriteSpec] =
                idColRewriteSpec::inpColMap.specs
            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)

            val outTargetExprs: List[AliasedExpr] = outColRewriteSpecs.map {
                spec => AliasedExpr(ColRef(spec.name), spec.alias)
            }

            val updOutExpr: RelExpr =
                RelOpExpr(Project(outTargetExprs), List(updExpr))

            (updOutExpr, outColMap)

        case (impute@Impute(imputeSpecs), List((inpExpr, inpColMap))) =>
            // need to add a project below and above the classification
            // because the classifier needs the original feature names
            val inpTargetExprs: List[AliasedExpr] = inpColMap.specs.map {
                spec => AliasedExpr(spec.alias, ColRef(spec.name))
            }

            val updInpExpr: RelExpr =
                RelOpExpr(Project(inpTargetExprs), List(inpExpr))
            val updExpr: RelExpr = RelOpExpr(impute, List(updInpExpr))

            val outFlagColRewriteSpecs: List[ColRewriteSpec] =
                imputeSpecs.flatMap { spec =>
                    spec.flagColRefOpt.map { flagColRef =>
                        val flagColAlias: ColRef =
                            ColRef(Counter.nextSymbol("F"))
                        ColRewriteSpec(Nil, flagColRef.name, flagColAlias)
                    }
                }

            val outColRewriteSpecs: List[ColRewriteSpec] =
                outFlagColRewriteSpecs:::inpColMap.specs
            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)

            val outTargetExprs: List[AliasedExpr] = outColRewriteSpecs.map {
                spec => AliasedExpr(ColRef(spec.name), spec.alias)
            }

            val updOutExpr: RelExpr =
                RelOpExpr(Project(outTargetExprs), List(updExpr))

            (updOutExpr, outColMap)

        case (nlpRelOp@NlpRelOp(libOpt, langOpt, name, args, inputCol, _),
              List((inpExpr, inpColMap))) =>
            val updArgs: List[ScalExpr] =
                args.map { arg => normalizeScalExpr(arg, inpColMap) }
            val updInputCol: ColRef = normalizeColRef(inputCol, inpColMap)

            val updResultCols: List[ColRef] = nlpRelOp.resultCols.map {
                resCol => ColRef(Counter.nextSymbol("N"))
            }

            val outColRewriteSpecs: List[ColRewriteSpec] =
                inpColMap.specs:::nlpRelOp.resultCols.zip(updResultCols).map {
                    case (col, alias) => ColRewriteSpec(Nil, col.name, alias)
                }

            val outColMap: RelColMap = RelColMap(outColRewriteSpecs)

            val updNlpRelOp: NlpRelOp = NlpRelOp(
                libOpt, langOpt, name, updArgs, updInputCol, updResultCols
            )

            val updOutExpr: RelExpr = RelOpExpr(updNlpRelOp, List(inpExpr))

            (updOutExpr, outColMap)

        case (OrderedBy(sortExprs), List((inpExpr, inpColMap))) =>
            val updSortExprs: List[SortExpr] = sortExprs.map {
                case SortExpr(expr, sortDir, nullsOrder) =>
                    SortExpr(normalizeScalExpr(expr, inpColMap),
                             sortDir, nullsOrder)
            }

            val updOrder: OrderedBy = OrderedBy(updSortExprs)
            (RelOpExpr(updOrder, List(inpExpr), locIdOverrideOpt), inpColMap)

        case _ =>
            throw new RuntimeException("Cannot normalize operator " + op)
    }

    private def normalizeTargetExprs(
        targetExprs: List[ScalarTarget],
        colMap: RelColMap
    ): (List[ScalarTarget], RelColMap) =
        targetExprs.reverse.foldLeft (List[ScalarTarget](), RelColMap(Nil)) {
            case ((tExprs, relColMap), target) =>
                val updatedAlias: ColRef = ColRef(Counter.nextSymbol("P"))
                val updatedTarget: ScalarTarget = target match {
                    case AliasedExpr(expr, _) =>
                        val updatedExpr: ScalExpr =
                            normalizeScalExpr(expr, colMap)
                        AliasedExpr(updatedExpr, updatedAlias)
                    case RenameCol(col, _) =>
                        val updatedCol: ColRef = normalizeColRef(col, colMap)
                        RenameCol(updatedCol, updatedAlias)
                    case ValueCol(v, _) =>
                        ValueCol(v, updatedAlias)
                }

                val spec: ColRewriteSpec =
                    ColRewriteSpec(Nil, target.alias.name, updatedAlias)

                (updatedTarget::tExprs, RelColMap(spec::relColMap.specs))
        }

    private def normalizeLabeler(
        labeler: RowLabeler,
        inpColMap: RelColMap
    ): RowLabeler = labeler match {
        case (constLabeler: ConstRowLabeler) => constLabeler

        case ColumnRowLabeler(colRef, whenThen, elseOpt, wild) =>
            val updColRef: ColRef = normalizeColRef(colRef, inpColMap)

            ColumnRowLabeler(updColRef, whenThen, elseOpt, wild)

        case (prl: PredRowLabeler) =>
            val updPredLabels: List[(ScalExpr, Label)] = prl.predLabels.map {
                case (pred, label) =>
                    (normalizeScalExpr(pred, inpColMap), label)
            }

            prl.clone(updPredLabels)

        case _ =>
            throw new RuntimeException("Cannot normalize labeler")
    }

    private def normalizeMatcher(
        matcher: RowSequenceMatcher,
        inpColMap: RelColMap
    ): (RowSequenceMatcher, RelColMap) = {
        val (updAggRowsSpec, outAggColMap) =
            normalizeAggregateRowsSpec(matcher.aggregateRowsSpec, inpColMap)

        val updPartnCols: List[ColRef] = matcher.partitionCols.map {
            colRef => normalizeColRef(colRef, inpColMap)
        }

        val updMatcher = matcher.clone(updAggRowsSpec, updPartnCols)

        val outColMap: RelColMap = matcher match {
            case (labelMatcher: LabelSequenceMatcher)
            if labelMatcher.isPartnColsInResult =>
                val partnSpecs: List[ColRewriteSpec] =
                    matcher.partitionCols.zip(updPartnCols).map {
                        case (col, alias) =>
                            ColRewriteSpec(Nil, col.name, alias)
                    }

                RelColMap(partnSpecs:::outAggColMap.specs)

            case _ => outAggColMap
        }

        (updMatcher, outColMap)
    }

    private def normalizeAggregateRowsSpec(
        spec: SeqAggregateRowsSpec,
        inpColMap: RelColMap
    ): (SeqAggregateRowsSpec, RelColMap) = spec match {
        case SeqArgOptsSpec(aggColSpecs) =>
            val (updAggColSpecs, _) =
                normalizeAggregateColSpecs(aggColSpecs, inpColMap)

            (SeqArgOptsSpec(updAggColSpecs), inpColMap)

        case SeqAggregateColSetSpec(aggColSpecs, retainedCols) =>
            val (updAggColSpecs, aggOutColMap) =
                normalizeAggregateColSpecs(aggColSpecs, inpColMap)

            val (updRetainedCols, retainedSpecs) =
                retainedCols.map { col =>
                    val alias: ColRef = normalizeColRef(col, inpColMap)
                    (alias, ColRewriteSpec(Nil, col.name, alias))
                } unzip

            val outColMap: RelColMap =
                RelColMap(retainedSpecs:::aggOutColMap.specs)

            (SeqAggregateColSetSpec(updAggColSpecs, updRetainedCols), outColMap)
    }

    private def normalizeAggregateColSpecs(
        aggColSpecs: List[SeqAggregateColSpec],
        inpColMap: RelColMap
    ): (List[SeqAggregateColSpec], RelColMap) = {
        val updAggColSpecMaps: List[(SeqAggregateColSpec, RelColMap)] =
            aggColSpecs.map { a => normalizeAggregateColSpec(a, inpColMap) }
        val (updAggColSpecs, outColMaps) = updAggColSpecMaps.unzip

        val outColMap: RelColMap =
            RelColMap(outColMaps.flatMap { cm => cm.specs })

        (updAggColSpecs, outColMap)
    }

    private def normalizeAggregateColSpec(
        spec: SeqAggregateColSpec,
        inpColMap: RelColMap
    ): (SeqAggregateColSpec, RelColMap) = spec match {
        case SeqColumnSpec(resultColRef, colRef, index, labels) =>
            val updResultColRef: ColRef = ColRef(Counter.nextSymbol("A"))
            val updColRef: ColRef = normalizeColRef(colRef, inpColMap)

            val updSpec: SeqColumnSpec = SeqColumnSpec(
                updResultColRef, updColRef, index, labels
            )

            val outColMap: RelColMap = RelColMap(
                List(ColRewriteSpec(Nil, resultColRef.name, updResultColRef))
            )

            (updSpec, outColMap)

        case SeqFunctionSpec(resultColRef, funcName, params, labels) =>
            val updResultColRef: ColRef = ColRef(Counter.nextSymbol("A"))
            val updParams: List[ScalExpr] =
                params.map { param => normalizeScalExpr(param, inpColMap) }

            val updSpec: SeqFunctionSpec = SeqFunctionSpec(
                updResultColRef, funcName, updParams, labels
            )

            val outColMap: RelColMap = RelColMap(
                List(ColRewriteSpec(Nil, resultColRef.name, updResultColRef))
            )

            (updSpec, outColMap)
    }

    private def rewriteColRef(
        colRef: ColRef
    ): (ColRef, ColRewriteSpec) = {
        val updColRef: ColRef = ColRef(Counter.nextSymbol("G"))
        (updColRef, ColRewriteSpec(Nil, colRef.name, updColRef))
    }

    private def normalizeScalExpr(
        scalExpr: ScalExpr,
        colRename: RelColMap
    ): ScalExpr = {
        val normScalExpr: ScalExpr =
            normalizeScalExprHelper(scalExpr, colRename)

        if( normScalExpr.isConstant )
            scalExprEvaluator.eval(normScalExpr)
        else normScalExpr
    }

    private def normalizeScalExprHelper(
        expr: ScalExpr,
        colRename: RelColMap
    ): ScalExpr = expr match {
        case ScalOpExpr(op, inputs) =>
            val updatedInputs: List[ScalExpr] =
                inputs.map { input => normalizeScalExpr(input, colRename) }
            ScalOpExpr(op, updatedInputs)

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            val updatedArgExpr: ScalExpr = normalizeScalExpr(argExpr, colRename)
            val updatedWhenThen: List[(ScalExpr, ScalExpr)] = whenThen.map {
                case (w, t) =>
                    (normalizeScalExpr(w, colRename),
                     normalizeScalExpr(t, colRename))
            }

            val updatedDefaultExpr: ScalExpr =
                normalizeScalExpr(defaultExpr, colRename)
            CaseExpr(updatedArgExpr, updatedWhenThen, updatedDefaultExpr)

        case (col: ColRefBase) =>
            normalizeColRefBase(col, colRename)

        case (value: ScalValue) => value

        case ScalSubQuery(relExpr) =>
            ScalSubQuery(normalizeRelExpr(relExpr, relExpr.locationIdOpt))

        case Exists(relExpr) =>
            Exists(normalizeRelExpr(relExpr, relExpr.locationIdOpt))

        case ScalCmpRelExpr(qual, subQueryOrList) =>
            ScalCmpRelExpr(qual, normalizeRelSubQuery(subQueryOrList))

        case _ =>
            throw new RuntimeException("Cannot normalize scalar expr: " + expr)
    }

    private def normalizeRelSubQuery(
        expr: RelSubQueryBase
    ): RelSubQueryBase = expr match {
        case (scalarList: ScalarList) => scalarList

        case RelSubQuery(query) =>
            normalizeRelExpr(query, query.locationIdOpt) match {
                case Values(_, rows@(row::_)) if row.scalars.size == 1 =>
                    val scalars: List[ScalColValue] = rows.map {
                        case Row(List(v)) => v
                        case r =>
                            throw new IllegalArgumentException(
                                "Found \"" + r.repr +
                                "\", expected singleton value"
                            )
                    }

                    ScalarList(scalars)

                case relExpr =>
                    RelSubQuery(relExpr)
            }

        case _ =>
            throw new RuntimeException("Cannot normalize subquery: " + expr)
    }

    private def normalizeColRefBase(
        col: ColRefBase,
        colMap: RelColMap
    ): ColRefBase = {
        val spec: ColRewriteSpec = colMap(col)

        col match {
            case ColRef(_) =>
                spec.alias
            case AnnotColRef(None, _) =>
                spec.alias
            case AnnotColRef(Some(tname), _) =>
                if( isSeqEnabled && spec.isMatchCol )
                    AnnotColRef(Some(tname), spec.alias.name)
                else spec.alias
            case LabeledColRef(Nil, None, _) =>
                spec.alias
            case LabeledColRef(List(label), None, _) =>
                if( isSeqEnabled && spec.isMatchCol )
                    AnnotColRef(Some(label), spec.alias.name)
                else spec.alias
            case LabeledColRef(labels, indexOpt, _) =>
                LabeledColRef(labels, indexOpt, spec.alias.name)
        }
    }

    private def normalizeColRef(
        col: ColRef,
        colMap: RelColMap
    ): ColRef = {
        val spec: ColRewriteSpec = colMap(col)
        spec.alias
    }

    // sequence function specification
    sealed abstract class FunctionSpec {
        val labels: List[String]
        // underlying columns
        val cols: List[ColRef]
    }

    case class LabeledColSpec(
        override val labels: List[String],
        indexOpt: Option[Int],
        colName: String
    ) extends FunctionSpec {
        override val cols: List[ColRef] = List(ColRef(colName))
    }

    case class AggregateSpec(
        override val labels: List[String],
        funcName: String,
        params: List[ScalExpr]
    ) extends FunctionSpec {
        override val cols: List[ColRef] =
            params.flatMap { param => param.colRefs }
    }

    sealed abstract class ColMap

    class RelColMap(
        val specs: List[ColRewriteSpec]
    ) extends ColMap {
        private lazy val colIndex: Map[String, List[ColRewriteSpec]] =
            specs.distinct.groupBy { spec => spec.name.toUpperCase }

        def apply(colName: String): ColRewriteSpec = apply(ColRef(colName))

        def apply(col: ColRefBase): ColRewriteSpec = candidates(col) match {
            case List(spec) => spec
            case Nil =>
                throw new IllegalArgumentException(
                    "Column \"" + col.repr + "\" not found"
                )
            case _ =>
                throw new IllegalArgumentException(
                    "Column \"" + col.repr + "\" is ambiguous"
                )
        }

        def candidates(col: ColRefBase): List[ColRewriteSpec] = col match {
            case ColRef(name) => candidates(name)

            case AnnotColRef(None, cname) => candidates(cname)

            case AnnotColRef(Some(tname), cname) =>
                candidates(cname).filter { spec => spec.labels contains tname }

            case LabeledColRef(labels, _, cname) =>
                candidates(cname).filter { spec =>
                    labels.diff(spec.labels).isEmpty
                }
        }

        private def candidates(name: String): List[ColRewriteSpec] =
            colIndex.get(name.toUpperCase) getOrElse Nil

        override def toString: String = "(" + specs.mkString(", ") + ")"
    }

    object RelColMap {
        def apply(specs: List[ColRewriteSpec]): RelColMap = new RelColMap(specs)
    }

    // gives the alias of the name appearing with the given labels as qualifiers
    class ColRewriteSpec(
        val labels: List[String],
        val name: String,
        val alias: ColRef,
        val isMatchCol: Boolean
    ) {
        override def toString: String =
            "[%s(%s, %s) -> %s]".format(
                if( isMatchCol ) "MATCH" else "",
                labels.mkString(":"),
                name,
                alias.name
            )
    }

    object ColRewriteSpec {
        def apply(
            labels: List[String],
            name: String,
            alias: ColRef,
            isMatchCol: Boolean = false
        ): ColRewriteSpec = new ColRewriteSpec(labels, name, alias, isMatchCol)
    }
}
