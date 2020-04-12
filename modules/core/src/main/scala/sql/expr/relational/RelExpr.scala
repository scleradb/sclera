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

import java.io.{ObjectInputStream, ObjectOutputStream, NotSerializableException}

import com.scleradb.exec.Schema
import com.scleradb.dbms.location.LocationId

import com.scleradb.external.expr.ExternalSourceExpr
import com.scleradb.external.objects.ExternalSource

import com.scleradb.sql.types.SqlCharVarying
import com.scleradb.sql.result.TableResult
import com.scleradb.sql.objects._

/** Table/table-like (relational) expressions */
abstract class RelExpr extends LogicalExpr {
    /** Location of the result if materialized, None otherwise */
    val locationIdOpt: Option[LocationId]

    /** Is this expression root evaluable as a stream?  */
    val isStreamEvaluable: Boolean

    /** Is this expression evaluable?  */
    val isEvaluable: Boolean

    /** Sort order of the result of this expression */
    val resultOrder: List[SortExpr]

    /** Relational view of this expression */
    override val tableView: RelExpr = this
    /** Columns in the result of this expression */
    override val tableColRefs: List[ColRef]
    /** Table names in the scope of the result of this expression */
    val tableNames: List[String]

    /** Columns in the result of "select *" over this expression */
    val starColumns: List[AnnotColRef]

    /** Returns at most maxRows, retaining the order */
    def limit(maxRows: Int): RelExpr =
        RelOpExpr(LimitOffset(Some(maxRows), 0, resultOrder), List(this))
}

// relational expression with a name, containing named columns
abstract class RelBaseExpr extends RelExpr {
    val name: String

    override val locationIdOpt: Option[LocationId]
    override val isStreamEvaluable: Boolean
    override val isEvaluable: Boolean

    override val resultOrder: List[SortExpr]

    override val tableColRefs: List[ColRef]
    override lazy val tableNames: List[String] = List(name)
    override lazy val starColumns: List[AnnotColRef] =
        tableColRefs.map { col => AnnotColRef(Some(name), col.name) }
}

// explicit values (rows of scalars)
sealed abstract class ValuesBase extends RelBaseExpr

case class Values(
    override val schema: Schema,
    rows: List[Row]
) extends ValuesBase {
    if( rows.isEmpty )
        throw new IllegalArgumentException("Value list cannot be empty")

    private val nScalars: Int = rows.head.scalars.size

    rows.tail.foreach { row =>
        if( row.scalars.size != nScalars )
            throw new IllegalArgumentException(
               "Values \"(" + rows.head.repr + ")\" and \"(" +
               row.repr + ")\" differ in the number of columns"
            )
    }

    override val locationIdOpt: Option[LocationId] = None
    override val isStreamEvaluable: Boolean = true
    override val isEvaluable: Boolean = true

    override lazy val name: String = "T"
    override lazy val tableColRefs: List[ColRef] =
        (1 to nScalars).toList.map { i => ColRef("V" + i) }

    override val resultOrder: List[SortExpr] = Nil
}

case class ResultValues(
    override val schema: Schema,
    tableResult: TableResult
) extends ValuesBase {
    override val locationIdOpt: Option[LocationId] = None
    override val isStreamEvaluable: Boolean = true
    override val isEvaluable: Boolean = true

    override lazy val name: String = "T"
    override lazy val tableColRefs: List[ColRef] =
        tableResult.columns.map { col => ColRef(col.name) }

    override val resultOrder: List[SortExpr] = tableResult.resultOrder
}

// references a table or view
trait RelRefSource extends RelBaseExpr {
    val name: String
    val aliasCols: List[ColRef]

    validate()

    // check for repeated aliases
    private def validate(): Unit = {
        val repeated: List[ColRef] = aliasCols diff aliasCols.distinct
        if( !repeated.isEmpty ) {
            throw new IllegalArgumentException(
                "Found repeated aliases: " +
                repeated.distinct.map(col => col.repr).mkString(", ")
            )
        }
    }
}

// base table reference
sealed abstract class TableRef extends RelBaseExpr {
    val schemaTable: SchemaTable

    def table: Table = schemaTable.obj

    val tableId: TableId
    def locationId: LocationId = tableId.locationId

    override lazy val locationIdOpt: Option[LocationId] = Some(locationId)
    override val isStreamEvaluable: Boolean = false
    override val isEvaluable: Boolean = true

    override val resultOrder: List[SortExpr] = Nil
}

// tableref as target
trait TableRefTarget extends TableRef {
    val name: String
    val targetCols: List[ColRef]

    validate()

    // check for repeated aliases
    private def validate(): Unit = {
        val repeated: List[ColRef] = targetCols diff targetCols.distinct
        if( !repeated.isEmpty ) {
            throw new IllegalArgumentException(
                "Found repeated target columns: " +
                repeated.distinct.map(col => col.repr).mkString(", ")
            )
        }
    }

    override lazy val tableColRefs: List[ColRef] =
        targetCols:::(table.columnRefs diff targetCols)

    // aligns the input list, assumed in permutedCols order, to table.columnRefs
    def align[T](xs: List[T]): List[T] = {
        if( tableColRefs.size != xs.size ) {
            throw new IllegalArgumentException(
                "Found a list with " + xs.size +
                " elements, was expecting " + tableColRefs.size
            )
        }

        val permutationMap: Map[ColRef, T] = Map() ++ (tableColRefs zip xs)

        tableColRefs.map { col =>
            permutationMap.get(col) getOrElse {
                throw new IllegalArgumentException(
                    "Element for column \"" + col.repr + "\" not specified"
                )
            }
        }
    }
}

trait TableRefSource extends TableRef with RelRefSource {
    override lazy val tableColRefs: List[ColRef] =
        aliasCols:::table.columnRefs.drop(aliasCols.size)
}

sealed abstract class TableRefByName extends TableRef {
    // needs to be lazy because this tableref might not exist yet
    override lazy val schemaTable: SchemaTable =
        SchemaTable.objectsByName(schema, name) match {
            case List(st) => st
            case Nil => throw new IllegalArgumentException(
                "Table \"" + name + "\" not found"
            )
            case sts => throw new IllegalArgumentException(
                "Found multiple tables with name \"" + name + "\" " +
                "(locations: " +
                    sts.map(st => st.locationId.name).mkString(", ") +
                ")"
            )
        }

    override lazy val tableId: TableId = schemaTable.id
}

case class TableRefTargetByName(
    override val schema: Schema,
    override val name: String,
    override val targetCols: List[ColRef] = Nil
) extends TableRefByName with TableRefTarget

case class TableRefSourceByName(
    override val schema: Schema,
    override val name: String,
    override val aliasCols: List[ColRef] = Nil
) extends TableRefByName with TableRefSource

sealed abstract class TableRefById extends TableRef {
    // needs to be lazy because this tableref might not exist yet
    override lazy val schemaTable: SchemaTable =
        SchemaTable.objectOpt(schema, tableId) match {
            case Some(st) => st
            case None => throw new IllegalArgumentException(
                "Table \"" + tableId.name + "\" not found " + 
                "in location \"" + tableId.locationId.repr + "\""
            )
        }

    override val name: String = tableId.name
}

case class TableRefTargetById(
    override val schema: Schema,
    override val tableId: TableId,
    override val targetCols: List[ColRef] = Nil
) extends TableRefById with TableRefTarget

case class TableRefSourceById(
    override val schema: Schema,
    override val tableId: TableId,
    override val aliasCols: List[ColRef] = Nil
) extends TableRefById with TableRefSource

sealed abstract class TableRefByIdString extends TableRef {
    val tableIdStr: String

    // needs to be lazy because this tableref could be a part
    // of the named table's definition
    override lazy val schemaTable: SchemaTable =
        SchemaTable.objectOpt(schema, tableIdStr) match {
            case Some(st) => st
            case None => throw new RuntimeException(
                "Table identifier \"" + tableIdStr + "\" not found"
            )
        }

    override lazy val tableId: TableId = schemaTable.id
    override lazy val name: String = schemaTable.obj.name
}

case class TableRefTargetByIdString(
    override val schema: Schema,
    override val tableIdStr: String,
    override val targetCols: List[ColRef] = Nil
) extends TableRefByIdString with TableRefTarget

case class TableRefSourceByIdString(
    override val schema: Schema,
    override val tableIdStr: String,
    override val aliasCols: List[ColRef] = Nil
) extends TableRefByIdString with TableRefSource

sealed abstract class TableRefExplicit extends TableRef {
    override val name: String = schemaTable.obj.name
    override val tableId: TableId = schemaTable.id
}

case class TableRefTargetExplicit(
    override val schema: Schema,
    override val schemaTable: SchemaTable,
    override val targetCols: List[ColRef] = Nil
) extends TableRefExplicit with TableRefTarget

case class TableRefSourceExplicit(
    override val schema: Schema,
    override val schemaTable: SchemaTable,
    override val aliasCols: List[ColRef] = Nil
) extends TableRefExplicit with TableRefSource

// view reference
sealed abstract class ViewRef extends RelBaseExpr with RelRefSource {
    val schemaView: SchemaView
    val viewId: ViewId

    def view: View = schemaView.obj

    override lazy val locationIdOpt: Option[LocationId] =
        view.expr.locationIdOpt
    override lazy val isStreamEvaluable: Boolean = view.expr.isStreamEvaluable
    override val isEvaluable: Boolean = view.expr.isEvaluable

    override lazy val resultOrder: List[SortExpr] = view.expr.resultOrder

    private lazy val rewrite: RelExpr =
        RelOpExpr(TableAlias(name, aliasCols), List(view.expr))

    override lazy val tableColRefs: List[ColRef] =
        aliasCols:::view.expr.tableColRefs.drop(aliasCols.size).map {
            col => ColRef(col.name)
        }
}

case class ViewRefByName(
    override val schema: Schema,
    override val name: String,
    override val aliasCols: List[ColRef] = Nil
) extends ViewRef {
    override lazy val schemaView: SchemaView =
        SchemaView.objectsByName(schema, name) match {
            case List(sv) => sv
            case Nil => throw new IllegalArgumentException(
                "View \"" + name + "\" not found"
            )
            case svs => throw new IllegalArgumentException(
                "Found multiple views with name \"" + name + "\""
            )
        }

    override lazy val viewId: ViewId = schemaView.id
}

case class ViewRefById(
    override val schema: Schema,
    override val viewId: ViewId,
    override val aliasCols: List[ColRef] = Nil
) extends ViewRef {
    override lazy val schemaView: SchemaView =
        SchemaView.objectOpt(schema, viewId) getOrElse {
            throw new IllegalArgumentException(
                "View \"" + viewId.repr + "\" not found"
            )
        }

    override val name: String = viewId.name
}

case class ViewRefExplicit(
    override val schemaView: SchemaView,
    override val aliasCols: List[ColRef] = Nil
) extends ViewRef {
    override val schema: Schema = schemaView.obj.expr.schema
    override val name: String = schemaView.obj.name
    override val viewId: ViewId = schemaView.id
}

// relational expression tree
case class RelOpExpr(
    op: RelOp,
    inputs: List[RelExpr],
    locIdOverrideOpt: Option[LocationId] = None
) extends RelExpr {
    require(inputs.length == op.arity, "Expecting " + op.arity + " inputs")

    override val schema: Schema = inputs.head.schema

    override lazy val locationIdOpt: Option[LocationId] =
        locIdOverrideOpt.filter { locId =>
            op.isLocEvaluable(locId.location(schema))
        } orElse op.locationIdOpt(inputs)

    override lazy val isStreamEvaluable: Boolean = op.isStreamEvaluable(inputs)

    override lazy val isEvaluable: Boolean =
        inputs.forall { inp => inp.isEvaluable } &&
        (!locationIdOpt.isEmpty || isStreamEvaluable)

    override lazy val resultOrder: List[SortExpr] = op.resultOrder(inputs)

    override lazy val tableColRefs: List[ColRef] = op.tableColRefs(inputs)

    override lazy val tableNames: List[String] = op.tableNames(inputs)

    override lazy val starColumns: List[AnnotColRef] = op.starColumns(inputs)
}

// helps instantiate a table/view when not clear from context
object RelExpr {
    def tableRef(
        schema: Schema,
        locId: LocationId,
        name: String
    ): TableRefSourceById = tableRef(schema, locId, name, Nil)

    def tableRef(
        schema: Schema,
        locId: LocationId,
        name: String,
        aliasCols: List[ColRef]
    ): TableRefSourceById =
        TableRefSourceById(schema, TableId(locId, name), aliasCols)

    def viewRef(
        schema: Schema,
        name: String,
        aliasCols: List[ColRef]
    ): ViewRefById =
        ViewRefById(schema, ViewId(name), aliasCols)

    def relRefSource(
        schema: Schema,
        name: String
    ): RelRefSource = relRefSource(schema, name, Nil)

    def relRefSource(
        schema: Schema,
        name: String,
        aliasCols: List[ColRef]
    ): RelRefSource =
        SchemaView.objectOpt(schema, ViewId(name)) match {
            case Some(sv) => // found a view with the name
                ViewRefExplicit(sv, aliasCols)
            case None => // did not find a view, check for a table
                TableRefSourceByName(schema, name, aliasCols)
        }

    def relRefSource(
        schema: Schema,
        locIdOpt: Option[LocationId],
        name: String
    ): RelRefSource = relRefSource(schema, locIdOpt, name, Nil)

    def relRefSource(
        schema: Schema,
        locIdOpt: Option[LocationId],
        name: String,
        aliasCols: List[ColRef]
    ): RelRefSource = locIdOpt match {
        case Some(locId) => tableRef(schema, locId, name, aliasCols)
        case None => relRefSource(schema, name, aliasCols)
    }

    def relRefSource(
        schema: Schema,
        relId: RelationId
    ): RelRefSource = relRefSource(schema, relId, Nil)

    def relRefSource(
        schema: Schema,
        relId: RelationId,
        aliasCols: List[ColRef]
    ): RelRefSource = relId match {
        case (tableId: TableId) =>
            TableRefSourceById(schema, tableId, aliasCols)
        case (viewId: ViewId) =>
            ViewRefById(schema, viewId, aliasCols)
    }

    def values(schema: Schema, rows: List[Row]): Values = {
        if( rows.isEmpty )
            throw new IllegalArgumentException("Value list cannot be empty")

        rows.tail.foreach { row =>
            if( row.scalars.size != rows.head.scalars.size )
                throw new IllegalArgumentException(
                   "Values \"(" + rows.head.repr + ")\" and \"(" +
                   row.repr + ")\" differ in the number of columns"
                )
        }

        Values(schema, rows)
    }

    def values(
        schema: Schema,
        rows: List[Row],
        alias: TableAlias
    ): RelExpr = RelOpExpr(alias, List(values(schema, rows)))

    def values(
        schema: Schema,
        rows: List[Row],
        aliasOpt: Option[TableAlias]
    ): RelExpr = aliasOpt match {
        case Some(alias) => values(schema, rows, alias)
        case None => values(schema, rows)
    }

    // singleton virtual table
    def singleton(schema: Schema): RelExpr =
        Values(schema, List(Row(List(SqlNull(SqlCharVarying(None))))))

    def singleton(schema: Schema, tname: String): RelExpr =
        RelOpExpr(TableAlias(tname), List(singleton(schema)))

    // empty virtual table
    def empty(schema: Schema): RelExpr =
        RelOpExpr(Select(BoolConst(false)), List(singleton(schema)))

    def empty(schema: Schema, tname: String): RelExpr =
        RelOpExpr(Select(BoolConst(false)), List(singleton(schema, tname)))

    def serialize(expr: RelExpr, out: ObjectOutputStream): Unit = expr match {
        case Values(_, rows) =>
            out.writeUTF("VALUES")
            out.writeInt(rows.size)
            rows.foreach { row => out.writeObject(row) }

        case (tRef: TableRefTarget) =>
            out.writeUTF("TABLEREFTARGET")
            out.writeObject(tRef.tableId)
            out.writeInt(tRef.targetCols.size)
            tRef.targetCols.foreach { col => out.writeObject(col) }

        case (tRef: TableRefSource) =>
            out.writeUTF("TABLEREFSOURCE")
            out.writeObject(tRef.tableId)
            out.writeInt(tRef.aliasCols.size)
            tRef.aliasCols.foreach { col => out.writeObject(col) }

        case (viewRef: ViewRef) =>
            out.writeUTF("VIEWREF")
            out.writeObject(viewRef.viewId)
            out.writeInt(viewRef.aliasCols.size)
            viewRef.aliasCols.foreach { col => out.writeObject(col) }

        case RelOpExpr(op, inputs, locIdOpt) =>
            out.writeUTF("RELOPEXPR")
            out.writeObject(op)
            out.writeBoolean(locIdOpt.isEmpty)
            locIdOpt.foreach { locId => out.writeObject(locId) }
            out.writeInt(inputs.size)
            inputs.foreach { inp => serialize(inp, out) }

        case ExternalSourceExpr(_, source) =>
            out.writeUTF("EXTERNALSOURCEEXPR")
            out.writeObject(source)

        case other =>
            throw new RuntimeException(
                "Unable to serialize relational expression: " + other
            )
    }

    def deSerialize(schema: Schema, in: ObjectInputStream): RelExpr =
        in.readUTF() match {
            case "VALUES" =>
                val nRows: Int = in.readInt()
                val rows: List[Row] =
                    (1 to nRows).toList.map { _ =>
                        in.readObject().asInstanceOf[Row]
                    }

                Values(schema, rows)

            case "TABLEREFTARGET" =>
                val tableId: TableId = in.readObject().asInstanceOf[TableId]
                val nTargetCols: Int = in.readInt()
                val targetCols: List[ColRef] =
                    (1 to nTargetCols).toList.map { _ =>
                        in.readObject().asInstanceOf[ColRef]
                    }
                TableRefTargetById(schema, tableId, targetCols)

            case "TABLEREFSOURCE" =>
                val tableId: TableId = in.readObject().asInstanceOf[TableId]
                val nAliasCols: Int = in.readInt()
                val aliasCols: List[ColRef] =
                    (1 to nAliasCols).toList.map { _ =>
                        in.readObject().asInstanceOf[ColRef]
                    }
                TableRefSourceById(schema, tableId, aliasCols)

            case "VIEWREF" =>
                val viewId: ViewId = in.readObject().asInstanceOf[ViewId]
                val nAliasCols: Int = in.readInt()
                val aliasCols: List[ColRef] =
                    (1 to nAliasCols).toList.map { _ =>
                        in.readObject().asInstanceOf[ColRef]
                    }
                ViewRefById(schema, viewId, aliasCols)

            case "RELOPEXPR" =>
                val op: RelOp = in.readObject().asInstanceOf[RelOp]
                val isLocIdNone: Boolean = in.readBoolean()
                val locIdOpt: Option[LocationId] =
                    if( isLocIdNone ) None
                    else Some(in.readObject().asInstanceOf[LocationId])
                val nInputs: Int = in.readInt()
                val inputs: List[RelExpr] =
                    (1 to nInputs).toList.map { _ => deSerialize(schema, in) }
                RelOpExpr(op, inputs)

            case "EXTERNALSOURCEEXPR" =>
                val source: ExternalSource =
                    in.readObject().asInstanceOf[ExternalSource]
                ExternalSourceExpr(schema, source)

            case other =>
                throw new RuntimeException(
                    "Unable to deserialize relational expression of type \"" +
                    other + "\""
                )
        }

    def orderLimitExpr(
        relExpr: RelExpr,
        orderOpt: Option[Order],
        limitOffsetFuncOpt: Option[List[SortExpr] => LimitOffset]
    ): RelExpr = {
        val (distinctOpt, distinctInput) = relExpr match {
            case RelOpExpr(distinct: DistinctBase, List(input), _) =>
                (Some(distinct), input)
            case _ => (None, relExpr)
        }

        val (projectOpt, outMapOpt, baseRelExpr) = distinctInput match {
            case RelOpExpr(aggregate: Aggregate, inputs@List(input), _) =>
                val outMap: Map[ScalExpr, ScalExpr] = Map() ++
                    aggregate.targetExprs.zipWithIndex.map { case(t, n) =>
                        (LongConst(n + 1) -> t.alias)
                    }

                (None, Some(outMap), RelOpExpr(aggregate, inputs))

            case RelOpExpr(project: Project, inputs@List(input), _) =>
                val outMap: Map[ScalExpr, ScalExpr] = Map() ++
                    project.targetExprs.zipWithIndex.flatMap { case(t, n) =>
                        val indexMap: (LongConst, ScalExpr) =
                            (LongConst(n + 1) -> t.expr)
                        val aliasMap: (ColRef, ScalExpr) = (t.alias -> t.expr)
                        List(indexMap, aliasMap)
                    }

                (Some(project), Some(outMap), input)

            case _ => (None, None, distinctInput)
        }

        val updSortExprs: List[SortExpr] = (orderOpt, outMapOpt) match {
            case (Some(Order(sortExprs)), Some(outMap)) =>
                sortExprs.map { case SortExpr(expr, dir, nOrder) =>
                    SortExpr(outMap.get(expr) getOrElse expr, dir, nOrder)
                }

            case (Some(Order(sortExprs)), None) => sortExprs

            case (None, _) => Nil
        }

        val updDistinctOpt: Option[DistinctBase] = distinctOpt.map {
            case Distinct => Distinct

            case DistinctOn(exprs, Nil) =>
                val updExprs: List[ScalExpr] = outMapOpt match {
                    case Some(outMap) =>
                        exprs.map { expr => outMap.get(expr) getOrElse expr }
                    case None => exprs
                }

                DistinctOn(updExprs, updSortExprs)

            case DistinctOn(_, sortExprs) =>
                throw new RuntimeException(
                    "Expecting empty ORDER in DISTINCT ON, found: " +
                    sortExprs.map(s => s.expr.repr).mkString("")
                )
        }

        val limitOffsetOpt: Option[LimitOffset] =
            limitOffsetFuncOpt.map { f => f(updSortExprs) }

        // eliminate order if subsumed by LIMIT or DISTINCT ON
        val projectInput: RelExpr =
            (updSortExprs, updDistinctOpt, limitOffsetOpt) match {
                case (Nil, None, None) =>
                    baseRelExpr

                case (sortExprs, None, None) =>
                    RelOpExpr(Order(sortExprs), List(baseRelExpr))

                case (Nil, Some(Distinct), None) =>
                    RelOpExpr(Distinct, List(baseRelExpr))

                case (sortExprs, Some(Distinct), None) =>
                    RelOpExpr(
                        Order(sortExprs),
                        List(RelOpExpr(Distinct, List(baseRelExpr)))
                    )

                case (_, Some(distinct: DistinctOn), None) =>
                    RelOpExpr(distinct, List(baseRelExpr))

                case (_, Some(distinct), Some(limitOffset)) =>
                    RelOpExpr(
                        limitOffset,
                        List(RelOpExpr(distinct, List(baseRelExpr)))
                    )

                case (_, None, Some(limitOffset)) =>
                    RelOpExpr(limitOffset, List(baseRelExpr))
            }

        projectOpt match {
            case Some(project) =>
                // evaluate the project after evaluation to
                // ensure that the ordering is retained
                val evalInput: RelExpr =
                    if( updSortExprs.isEmpty ||
                        projectInput.locationIdOpt.isEmpty ) projectInput
                    else RelOpExpr(EvaluateOp, List(projectInput))

                RelOpExpr(project, List(evalInput))

            case None => projectInput
        }
    }
}
