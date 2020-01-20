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

import com.scleradb.exec.Schema
import com.scleradb.sql.exec.ScalFunctionEvaluator
import com.scleradb.dbms.location.{LocationId, Location, ReadWriteLocation}

/** Abstract base class for all relational operators */
sealed abstract class RelOp {
    /** Arity of the operator (that is, number of inputs) */
    val arity: Int

    /** Is this operator evaluable over the input stream?
      * @param inputs Input expressions
      */
    def isStreamEvaluable(inputs: List[RelExpr]): Boolean

    /** Is this operator evaluable at the given location?
      * @param loc Location
      */
    def isLocEvaluable(loc: Location): Boolean

    /** The location where this operator can be evaluated
      * @param inputs Input expressions
      * @return Location where this operator can be evaluated (if any)
      */
    def locationIdOpt(inputs: List[RelExpr]): Option[LocationId]

    /** The sort order of the result of this operator
      * @param inputs Input expressions
      * @return Sort order of the result of this operator (if any)
      */
    def resultOrder(inputs: List[RelExpr]): List[SortExpr]

    /** The column names in the result of this operator
      * @param inputs Input expressions
      * @return Columns names in the result of this operator
      */
    def tableColRefs(inputs: List[RelExpr]): List[ColRef]

    /** The table names in the scope of the result of this operator
      * @param inputs Input expressions
      * @return Table names in the scope of the result of this operator
      */
    def tableNames(inputs: List[RelExpr]): List[String]

    /** The columns in the result of "select *" over this operator
      * @param inputs Input expressions
      * @return Columns in the result of "select *" over this operator
      */
    def starColumns(inputs: List[RelExpr]): List[AnnotColRef]
}

/** Abstract base class for regular (standard) relational operators */
sealed abstract class RegularRelOp extends RelOp

private[scleradb]
sealed abstract class ProjectBase extends RegularRelOp {
    override val arity: Int = 1

    val targetExprs: List[ScalarTarget]

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        targetExprs.map { t => t.alias }

    override def tableNames(inputs: List[RelExpr]): List[String] = Nil

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        tableColRefs(inputs).map { col => AnnotColRef(None, col.name) }

    override def resultOrder(
        inputs: List[RelExpr]
    ): List[SortExpr] = locationIdOpt(inputs) match {
        case Some(_) => Nil // materialized - order lost
        case None =>
            val input: RelExpr = inputs.head
            val targetMap: Map[ScalExpr, ColRef] = Map() ++
                targetExprs.map { t => t.expr -> t.alias }

            val sortExprs: List[SortExpr] = input.resultOrder.takeWhile {
                case SortExpr(expr, _, _) => (targetMap contains expr)
            }

            sortExprs.map { case SortExpr(expr, sortDir, nOrder) =>
                SortExpr(targetMap(expr), sortDir, nOrder)
            }
    }
}

private[scleradb]
case class Project(
    override val targetExprs: List[ScalarTarget]
) extends ProjectBase {
    def isComplexTarget: Boolean =
        targetExprs.exists { t =>
            t.expr match {
                case (_: ColRef) => false
                case ScalOpExpr(_: TypeCast, List(_: ColRef)) => false
                case (_: ScalColValue) => false
                case ScalOpExpr(_: TypeCast, List(_: ScalColValue)) => false
                case _ => true
            }
        }

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean =
        targetExprs.forall { t =>
            ScalFunctionEvaluator.isStreamEvaluable(t.expr)
        }

    override def isLocEvaluable(loc: Location): Boolean =
        targetExprs.forall { t =>
            ScalFunctionEvaluator.isSupported(loc, t.expr)
        }

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] =
        inputs.head.locationIdOpt.filter { locId =>
            isLocEvaluable(locId.location(inputs.head.schema))
        }
}

private[scleradb]
case class Aggregate(
    override val targetExprs: List[ScalarTarget],
    groupExprs: List[ScalExpr] = Nil,
    predOpt: Option[ScalExpr] = None
) extends ProjectBase {
    private lazy val exprs: List[ScalExpr] =
        targetExprs.map { t => t.expr }:::groupExprs:::predOpt.toList

    // all columns?
    def isComplexGroupBy: Boolean =
        groupExprs.exists {
            case (_: ColRef) => false
            case _ => true
        }

    def streamPartitionExprs(inputs: List[RelExpr]): List[ScalExpr] = {
        val inpSortExprs: List[ScalExpr] =
            inputs.head.resultOrder.map { case SortExpr(expr, _, _) => expr }

        // included prefix
        inpSortExprs.takeWhile { expr => groupExprs contains expr }
    }

    // sort required if groupExprs is not a subset of partition exprs
    def requiresSort(inputs: List[RelExpr]): Boolean =
        groupExprs.distinct.size > streamPartitionExprs(inputs).distinct.size

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean =
        exprs.forall { expr =>
            ScalFunctionEvaluator.isStreamEvaluable(expr)
        } && !requiresSort(inputs)

    override def isLocEvaluable(loc: Location): Boolean =
        exprs.forall { expr => ScalFunctionEvaluator.isSupported(loc, expr) }

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] =
        inputs.head.locationIdOpt.filter { locId =>
            isLocEvaluable(locId.location(inputs.head.schema))
        }
}

private[scleradb]
object ProjectBase {
    def apply(
        targetExprs: List[TargetExpr],
        input: RelExpr,
        groupExprsOpt: Option[List[ScalExpr]],
        havingPredOpt: Option[ScalExpr]
    ): ProjectBase = {
        val updTargetExprs: List[ScalarTarget] =
            expandedTargetExprs(targetExprs, input)

        val isAggExprs: Boolean =
            updTargetExprs.exists { expr => expr.isAggregate }

        if( groupExprsOpt.isEmpty && havingPredOpt.isEmpty && !isAggExprs )
            Project(updTargetExprs)
        else {
            // substitute constants and result aliases in group exprs
            val groupExprs: List[ScalExpr] = groupExprsOpt match {
                case Some(exprs) =>
                    val outMap: Map[ScalExpr, ScalExpr] = Map() ++
                        updTargetExprs.zipWithIndex.flatMap { case(t, n) =>
                            val indexMap: (LongConst, ScalExpr) =
                                (LongConst(n + 1) -> t.expr)
                            if( input.tableColRefs contains t.alias )
                                List(indexMap)
                            else {
                                val aliasMap: (ColRef, ScalExpr) =
                                    (t.alias -> t.expr)
                                List(indexMap, aliasMap)
                            }
                        }

                    exprs.map { expr => outMap.get(expr) getOrElse expr }

                case None => Nil
            }

            Aggregate(updTargetExprs, groupExprs, havingPredOpt)
        }
    }

    def expandedTargetExprs(
        targetExprs: List[TargetExpr],
        input: RelExpr
    ): List[ScalarTarget] = targetExprs.flatMap {
        case (scalarTarget: ScalarTarget) => List(scalarTarget)
        case star@StarTargetExpr(tIndexOpt, exceptCols) =>
            val filteredInpStarCols: List[AnnotColRef] = tIndexOpt match {
                case None => input.starColumns
                case Some((t, _)) =>
                    val starCols: List[AnnotColRef] = input.starColumns.filter {
                        case AnnotColRef(Some(tname), _) => (t == tname)
                        case _ => input.tableNames contains t
                    }

                    if( starCols.isEmpty ) {
                        throw new IllegalArgumentException(
                            "Cannot resolve qualifier " + t
                        )
                    }

                    starCols
            }

            val filteredInpStarColNames: List[String] =
                filteredInpStarCols.map { case AnnotColRef(_, cname) => cname }

            val exceptColNames: List[String] =
                exceptCols.map { col => col.name }

            val badColNames: List[String] =
                exceptColNames diff filteredInpStarColNames
            if( !badColNames.isEmpty )
                throw new IllegalArgumentException(
                    (if( badColNames.size == 1 ) "Column" else "Columns") +
                    " " + badColNames.mkString(", ") + " not found"
                )

            val starCols: List[AnnotColRef] = filteredInpStarCols.filter {
                case AnnotColRef(_, colName) =>
                    !(exceptColNames contains colName)
            }

            tIndexOpt match {
                case Some((t, indexOpt@Some(_))) =>
                    starCols.map { case annot@AnnotColRef(_, name) =>
                        AliasedExpr(
                            LabeledColRef(List(t), indexOpt, name),
                            ColRef(annot.defaultAlias)
                        )
                    }

                case Some((t, None)) =>
                    starCols.map { case annot@AnnotColRef(_, name) =>
                        AliasedExpr(
                            AnnotColRef(Some(t), name),
                            ColRef(annot.defaultAlias)
                        )
                    }

                case None =>
                    starCols.map {
                        case annot@AnnotColRef(None, name) =>
                            RenameCol(ColRef(name), ColRef(name))
                        case annot =>
                            AliasedExpr(annot, ColRef(annot.defaultAlias))
                    }
            }
        }
}

/** Operators with a sorting requirement */
trait SortRelOp extends RelOp {
    val sortExprs: List[SortExpr]
}

/** Sort order specification
  *
  * @param sortExprs Sort expressions that determine the sort order
  */
case class Order(
    override val sortExprs: List[SortExpr]
) extends RegularRelOp with SortRelOp {
    override val arity: Int = 1

    def isSubsumedBy(order: List[SortExpr]): Boolean =
        SortExpr.isSubsumedBy(sortExprs, order)

    def isSubsumedBy(order: Order): Boolean =
        isSubsumedBy(order.sortExprs)

    def isSubsumedBy(orderOpt: Option[Order]): Boolean =
        orderOpt match {
            case Some(order) => isSubsumedBy(order)
            case None => false
        }

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean =
        isSubsumedBy(inputs.head.resultOrder)

    override def isLocEvaluable(loc: Location): Boolean =
        sortExprs.forall { case SortExpr(expr, _, _) =>
            ScalFunctionEvaluator.isSupported(loc, expr)
        }

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] =
        inputs.head.locationIdOpt.filter { locId =>
            isLocEvaluable(locId.location(inputs.head.schema))
        }

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] = sortExprs

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs

    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.head.tableNames

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns

    def compatiblePartnCols(partnCols: List[ColRef]): List[ColRef] =
        SortExpr.compatiblePartnCols(sortExprs, partnCols)
}

private[scleradb]
case class LimitOffset(
    limitOpt: Option[Int],
    offset: Int,
    override val sortExprs: List[SortExpr]
) extends RegularRelOp with SortRelOp {
    override val arity: Int = 1

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean =
        SortExpr.isSubsumedBy(sortExprs, inputs.head.resultOrder)

    override def isLocEvaluable(loc: Location): Boolean =
        sortExprs.forall { case SortExpr(expr, _, _) =>
            ScalFunctionEvaluator.isSupported(loc, expr)
        }

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] =
        inputs.head.locationIdOpt.filter { locId =>
            isLocEvaluable(locId.location(inputs.head.schema))
        }

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        sortExprs match {
            case Nil =>
                locationIdOpt(inputs) match {
                    case Some(_) => Nil
                    case None => inputs.head.resultOrder
                }
            case sortExprs => sortExprs
        }

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs

    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.head.tableNames

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns
}

private[scleradb]
case class TableAlias(
    name: String,
    cols: List[ColRef] = Nil,
    partitionCols: List[ColRef] = Nil
) extends RegularRelOp {
    override val arity: Int = 1

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    override def isLocEvaluable(loc: Location): Boolean = true

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] =
        inputs.head.locationIdOpt

    override def resultOrder(
        inputs: List[RelExpr]
    ): List[SortExpr] = {
        // order containing complex sort expressions is ignored
        val exprMap: Map[ScalExpr, ColRef] = Map() ++
            (inputs.head.tableColRefs zip tableColRefs(inputs))

        val validSortExprs: List[SortExpr] =
            inputs.head.resultOrder.takeWhile {
                case SortExpr(sortExpr, _, _) => exprMap contains sortExpr
            }

        validSortExprs.map { case SortExpr(sortExpr, sortDir, nOrder) =>
            SortExpr(exprMap(sortExpr), sortDir, nOrder)
        }
    }

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] = {
        val inpCols: List[ColRef] = inputs.head.tableColRefs

        if( cols.size > inpCols.size )
            throw new IllegalArgumentException(
                "Table alias contains extra columns: " +
                cols.drop(inpCols.size).map(col => col.repr).mkString(", ")
            )

        cols ++ inpCols.drop(cols.size)
    }

    override def tableNames(inputs: List[RelExpr]): List[String] = List(name)

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        tableColRefs(inputs).map { col => AnnotColRef(Some(name), col.name) }
}

private[scleradb]
sealed abstract class DistinctBase extends RegularRelOp {
    override val arity: Int = 1

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs

    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.head.tableNames

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns
}

private[scleradb]
case class DistinctOn(
    exprs: List[ScalExpr],
    override val sortExprs: List[SortExpr]
) extends DistinctBase with SortRelOp {
    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean =
        exprs.forall { e => ScalFunctionEvaluator.isStreamEvaluable(e) } &&
        SortExpr.isSubsumedBy(sortExprs, inputs.head.resultOrder)

    override def isLocEvaluable(loc: Location): Boolean =
        exprs.forall { expr =>
            ScalFunctionEvaluator.isSupported(loc, expr)
        } &&
        sortExprs.forall { case SortExpr(expr, _, _) =>
            ScalFunctionEvaluator.isSupported(loc, expr)
        }

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] =
        inputs.head.locationIdOpt.filter { locId =>
            val loc: Location = locId.location(inputs.head.schema)
            isLocEvaluable(locId.location(inputs.head.schema))
        }

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] = sortExprs
}

private[scleradb]
case object Distinct extends DistinctBase {
    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = {
        val nCols: Int = inputs.head.tableColRefs.size
        val sortExprs: List[ScalExpr] =
            inputs.head.resultOrder.take(nCols).map {
                case SortExpr(expr, _, _) => expr 
            }

        inputs.head.tableColRefs.diff(sortExprs).isEmpty
    }

    override def isLocEvaluable(loc: Location): Boolean = true

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] =
        inputs.head.locationIdOpt

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] = Nil
}

private[scleradb]
case class Select(predExpr: ScalExpr) extends RegularRelOp {
    override val arity: Int = 1

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean =
        ScalFunctionEvaluator.isStreamEvaluable(predExpr)

    override def isLocEvaluable(loc: Location): Boolean =
        ScalFunctionEvaluator.isSupported(loc, predExpr)

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] =
        inputs.head.locationIdOpt.filter { locId =>
            isLocEvaluable(locId.location(inputs.head.schema))
        }

    override def resultOrder(
        inputs: List[RelExpr]
    ): List[SortExpr] = locationIdOpt(inputs) match {
        case Some(_) => Nil // materialized - order lost
        case None => inputs.head.resultOrder
    }

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs

    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.head.tableNames

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns
}

private[scleradb]
case class Join(
    joinType: JoinType,
    joinPred: JoinPred
) extends RegularRelOp {
    override val arity: Int = 2

    override def isLocEvaluable(loc: Location): Boolean =
        (loc.permit == ReadWriteLocation) && { // to materialize fetched inputs
            joinPred match {
                case JoinOn(predExpr) =>
                    ScalFunctionEvaluator.isSupported(loc, predExpr)
                case _ => true
            }
        }

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] = {
        val targetLocIdOpts: List[Option[LocationId]] =
            inputs.map { input => input.locationIdOpt } distinct
        
        // assuming the LHS is the larger input
        if( targetLocIdOpts.headOption == Some(None) ) None else {
            targetLocIdOpts.flatten.find { locId =>
                isLocEvaluable(locId.location(inputs.head.schema))
            }
        }
    }

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean =
        (joinPred, inputs) match {
            case (JoinOn(ScalOpExpr(Equals, List(a: ColRef, b: ColRef))),
                  List(lhs, rhs)) =>
                lhs.tableColRefs.contains(a) && rhs.tableColRefs.contains(b) && 
                lhs.locationIdOpt.isEmpty && (
                    (joinType == LeftOuter) || (joinType == Inner) || (
                        lhs.resultOrder match {
                            case SortExpr(lcol: ColRef, _, _)::_ => (lcol == a)
                            case _ => false
                        }
                    )
                )

            case _ => false
        }

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        if( isStreamEvaluable(inputs) ) {
            joinType match {
                case (LeftOuter | Inner) =>
                    inputs.head.resultOrder
                case (RightOuter | FullOuter) =>
                    Nil // NULLs invalidate LHS join order
            }
        } else Nil

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] = {
        val inpCols: List[ColRef] =
            inputs.flatMap { input => input.tableColRefs }

        joinPred match {
            case JoinOn(_) => inpCols
            case JoinUsing(cols) => inpCols diff cols
            case JoinNatural => inpCols.distinct
        }
    }

    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.flatMap { input => input.tableNames }

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        joinPred match {
            case JoinOn(_) => inputs.flatMap { input => input.starColumns }
            case JoinUsing(cols) =>
                val colNames: List[String] = cols.map { col => col.name }
                joinType match {
                    case RightOuter =>
                        inputs.init.flatMap { inp =>
                            inp.starColumns.filter {
                                annot => !colNames.contains(annot.name)
                            }
                        } :::
                        inputs.last.starColumns
                    case _ =>
                        inputs.head.starColumns:::
                        inputs.tail.flatMap { inp =>
                            inp.starColumns.filter {
                                annot => !colNames.contains(annot.name)
                            }
                        }
                }
            case JoinNatural =>
                inputs.head.starColumns::: {
                    val colNames: List[String] =
                        inputs.head.starColumns.map { annot => annot.name }
                    inputs.tail.flatMap { inp =>
                        inp.starColumns.filter {
                            annot => !colNames.contains(annot.name)
                        }
                    }
                }
        }
}

// join type
private[scleradb]
sealed abstract class JoinType
private[scleradb]
case object Inner extends JoinType
private[scleradb]
case object FullOuter extends JoinType
private[scleradb]
case object LeftOuter extends JoinType
private[scleradb]
case object RightOuter extends JoinType

// join predicate
private[scleradb]
sealed abstract class JoinPred
private[scleradb]
case class JoinOn(predExpr: ScalExpr) extends JoinPred
private[scleradb]
case class JoinUsing(cols: List[ColRef]) extends JoinPred
private[scleradb]
case object JoinNatural extends JoinPred

// compound operator 
private[scleradb]
case class Compound(compoundType: CompoundType) extends RegularRelOp {
    override val arity: Int = 2

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    override def isLocEvaluable(loc: Location): Boolean =
        loc.permit == ReadWriteLocation

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] = {
        val targetLocIdOpts: List[Option[LocationId]] =
            inputs.map { input => input.locationIdOpt } distinct

        if( targetLocIdOpts contains None )
            None
        else if( targetLocIdOpts.size == 1 )
            targetLocIdOpts.head // all inputs are in the same location
        else if( compoundType == Union )
            None // merge the inputs in-memory as they are fetched
        else targetLocIdOpts.flatten.find { locId =>
            isLocEvaluable(locId.location(inputs.head.schema))
        }
    }

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] = Nil

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs

    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.head.tableNames

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns
}

// compound operator types
private[scleradb]
sealed abstract class CompoundType
private[scleradb]
case object Union extends CompoundType
private[scleradb]
case object Intersect extends CompoundType
private[scleradb]
case object Except extends CompoundType

/** Extended (non-standard) relational operators */
abstract class ExtendedRelOp extends RelOp {
    override def isLocEvaluable(loc: Location): Boolean = false

    override def locationIdOpt(inputs: List[RelExpr]): Option[LocationId] = None
}

private[scleradb]
case object EvaluateOp extends ExtendedRelOp {
    override val arity: Int = 1

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head.resultOrder

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs
    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.head.tableNames
    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns
}

/*
private[scleradb]
case class Materialize(locationId: LocationId) extends ExtendedRelOp {
    override val arity: Int = 1

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head match {
            case RelOpExpr(Order(sortExprs), _) => sortExprs
            case LimitOffset(_, _, sortExprs) => sortExprs
            case DistinctOn(_, sortExprs) => sortExprs
            case _ => Nil
        }

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs
    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.head.tableNames
    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns
}
*/

private[scleradb]
case class Align(
    distanceExpr: ScalExpr,
    marginOpt: Option[Int]
) extends ExtendedRelOp {
    override val arity: Int = 2

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head.resultOrder match {
            case Nil => inputs.tail.head.resultOrder
            case nonempty => nonempty
        }

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.flatMap { inp => inp.tableColRefs }
    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.flatMap { input => input.tableNames }
    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.flatMap { inp => inp.starColumns }
}

// create disjoint intervals from possibly overlapping intervals
private[scleradb]
case class DisjointInterval(
    inpLhsColRef: ColRef,
    inpRhsColRef: ColRef,
    outLhsColRef: ColRef,
    outRhsColRef: ColRef,
    partnColRefs: List[ColRef]
) extends ExtendedRelOp {
    override val arity: Int = 1

    // needs to the sorted on LHS on input intervals
    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = {
        val (partnOrder, partnRowsOrder) =
            inputs.head.resultOrder.span { case SortExpr(expr, _, _) =>
                partnColRefs contains expr
            }

        val partnExprs: List[ScalExpr] =
            partnOrder.map { case SortExpr(expr, _, _) => expr }

        (partnExprs.distinct.size == partnColRefs.distinct.size) && {
            partnRowsOrder.headOption match {
                case Some(SortExpr(col: ColRef, SortAsc, _)) =>
                    (col == inpLhsColRef)
                case _ => false
            }
        }
    }

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head.resultOrder.takeWhile { case SortExpr(expr, _, _) =>
            partnColRefs contains expr
        } ::: List(
            SortExpr(outLhsColRef, SortAsc, NullsLast),
            SortExpr(outRhsColRef, SortAsc, NullsLast)
        )

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        outLhsColRef::outRhsColRef::inputs.head.tableColRefs
    override def tableNames(inputs: List[RelExpr]): List[String] = Nil
    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        tableColRefs(inputs).map { col => AnnotColRef(None, col.name) }
}

private[scleradb]
case class UnPivot(
    outValCol: ColRef,
    outKeyCol: ColRef,
    inColVals: List[(ColRef, CharConst)]
) extends ExtendedRelOp {
    override val arity: Int = 1

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    private lazy val inCols: List[ColRef] =
        inColVals.map { case (col, _) => col }

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head.resultOrder.takeWhile { case SortExpr(expr, _, _) =>
            expr.colRefs.forall { col => !inCols.contains(col) }
        }

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        outValCol::outKeyCol::inputs.head.tableColRefs.diff(inColVals)

    override def tableNames(inputs: List[RelExpr]): List[String] = Nil
    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        tableColRefs(inputs).map { col => AnnotColRef(None, col.name) }
}

private[scleradb]
case class OrderedBy(sortExprs: List[SortExpr]) extends ExtendedRelOp {
    override val arity: Int = 1

    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] = sortExprs

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        inputs.head.tableColRefs
    override def tableNames(inputs: List[RelExpr]): List[String] =
        inputs.head.tableNames
    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        inputs.head.starColumns
}
