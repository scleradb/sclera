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

package com.scleradb.sql.statements

import com.scleradb.dbms.location.{LocationId, Location}
import com.scleradb.objects._
import com.scleradb.sql.objects._
import com.scleradb.sql.expr._

import com.scleradb.external.objects.ExternalTarget
import com.scleradb.analytics.sequence.labeler.RowLabeler

// Extended SQL statement
private[scleradb]
abstract class SqlStatement

private[scleradb]
abstract class SqlQueryStatement extends SqlStatement

// SQL query parsed into a relational expression
private[scleradb]
case class SqlRelQueryStatement(
    relExpr: RelExpr
) extends SqlQueryStatement {
    def resultOrder: List[SortExpr] = relExpr.resultOrder
}

// update statements
private[scleradb]
abstract class SqlUpdateStatement extends SqlStatement

// SQL/EXT CREATE/DROP variants
private[scleradb]
abstract class SqlUpdateSchema extends SqlUpdateStatement

private[scleradb]
case class SqlCreateDbSchema(dbSchema: String) extends SqlUpdateSchema

private[scleradb]
abstract class SqlCreate extends SqlUpdateSchema

private[scleradb]
case class SqlCreateExt(
    source: ExternalTarget,
    relExpr: RelExpr
) extends SqlCreate

private[scleradb]
abstract class SqlCreateObj extends SqlCreate {
    val duration: DbObjectDuration
}

private[scleradb]
case class SqlCreateDbObject(
    obj: SqlDbObject,
    override val duration: DbObjectDuration
) extends SqlCreateObj

private[scleradb]
case class SqlCreateMLObject(
    libOpt: Option[String],
    obj: SqlMLObject,
    trainRelExpr: RelExpr,
    override val duration: DbObjectDuration
) extends SqlCreateObj

private[scleradb]
abstract class SqlDrop extends SqlUpdateSchema {
    val objectId: SchemaObjectId
}

private[scleradb]
case class SqlDropById(
    override val objectId: SchemaObjectId
) extends SqlDrop

private[scleradb]
case class SqlDropExplicit(
    obj: SchemaObject,
    duration: DbObjectDuration
) extends SqlDrop {
    override val objectId: SchemaObjectId = obj.id
}

private[scleradb]
abstract class SqlUpdateTable extends SqlUpdateStatement {
    val tableId: TableId
}

private[scleradb]
abstract class SqlInsert extends SqlUpdateTable {
    val targetCols: List[ColRef]
}

private[scleradb]
case class SqlInsertQueryResult(
    override val tableId: TableId,
    override val targetCols: List[ColRef],
    query: RelExpr
) extends SqlInsert

private[scleradb]
case class SqlInsertValueRows(
    override val tableId: TableId,
    override val targetCols: List[ColRef],
    rows: List[Row]
) extends SqlInsert

private[scleradb]
object SqlInsert {
    def apply(
        tableId: TableId,
        targetCols: List[ColRef],
        rows: List[Row]
    ): SqlInsert = SqlInsertValueRows(tableId, targetCols, rows)

    def apply(
        tableRef: TableRefTarget,
        rows: List[Row]
    ): SqlInsert = {
        val tableId: TableId = tableRef.tableId
        val targetCols: List[ColRef] = rows.headOption match {
            case Some(Row(scalars)) => tableRef.tableColRefs.take(scalars.size)
            case None => Nil
        }

        apply(tableId, targetCols, rows)
    }

    def apply(
        tableId: TableId,
        targetCols: List[ColRef],
        relExpr: RelExpr
    ): SqlInsert = relExpr match {
        case Values(_, rows) => SqlInsertValueRows(tableId, targetCols, rows)
        case query => SqlInsertQueryResult(tableId, targetCols, query)
    }

    def apply(
        tableRef: TableRefTarget,
        relExpr: RelExpr
    ): SqlInsert = {
        val tableId: TableId = tableRef.tableId
        val targetCols: List[ColRef] =
            tableRef.tableColRefs.take(relExpr.tableColRefs.size)

        apply(tableId, targetCols, relExpr)
    }
}

private[scleradb]
case class SqlUpdate(
    override val tableId: TableId,
    colValPairs: List[(ColRef, ScalExpr)],
    pred: ScalExpr
) extends SqlUpdateTable

private[scleradb]
case class SqlDelete(
    override val tableId: TableId,
    pred: ScalExpr
) extends SqlUpdateTable

private[scleradb]
case class SqlUpdateBatch(
    stmts: List[SqlUpdateTable]
) extends SqlUpdateStatement

private[scleradb]
abstract class SqlIndex extends SqlUpdateStatement

private[scleradb]
case class SqlCreateIndex(
    name: String,
    tableId: TableId,
    indexCols: List[ColRef],
    pred: ScalExpr
) extends SqlIndex

private[scleradb]
case class SqlDropIndex(indexName: String) extends SqlIndex

private[scleradb]
abstract class SqlAlter extends SqlUpdateStatement

// native statement execution
private[scleradb]
case class SqlNativeStatement(
    locationId: LocationId,
    stmtStr: String
) extends SqlUpdateStatement

private[scleradb]
abstract class SqlAdminStatement extends SqlStatement

private[scleradb]
case object SqlCreateSchema extends SqlAdminStatement

private[scleradb]
case object SqlDropSchema extends SqlAdminStatement

private[scleradb]
case class SqlAddLocation(
    locationId: LocationId,
    dbname: String,
    params: List[String],
    dbSchemaOpt: Option[String],
    permitStrOpt: Option[String]
) extends SqlAdminStatement

private[scleradb]
case class SqlRemoveLocation(locationId: LocationId) extends SqlAdminStatement

private[scleradb]
case class SqlAddTable(
    tableId: TableId,
    tableOpt: Option[Table]
) extends SqlAdminStatement

private[scleradb]
case class SqlRemoveTable(tableId: TableId) extends SqlAdminStatement

private[scleradb]
case class SqlExplainScript(isExplain: Boolean) extends SqlAdminStatement

private[scleradb]
case class SqlConfigLocation(
    param: String,
    locationId: LocationId
) extends SqlAdminStatement

private[scleradb]
abstract class SqlAdminQueryStatement extends SqlStatement

private[scleradb]
case object SqlShowOptions extends SqlAdminQueryStatement

private[scleradb]
case object SqlShowConfig extends SqlAdminQueryStatement

private[scleradb]
case class SqlListRemainingTables(
    locIdOpt: Option[LocationId],
    format: Format
) extends SqlAdminQueryStatement

private[scleradb]
case class SqlListAddedTables(
    locIdOpt: Option[LocationId],
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

private[scleradb]
case class SqlListViews(
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

private[scleradb]
case class SqlListClassifiers(
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

private[scleradb]
case class SqlListClusterers(
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

private[scleradb]
case class SqlListObjects(
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

private[scleradb]
case object SqlListLocations extends SqlAdminQueryStatement

private[scleradb]
case class SqlExplainPlan(relExpr: RelExpr) extends SqlAdminQueryStatement

private[scleradb]
sealed abstract class SqlDbObject {
    val name: String
}

private[scleradb]
case class SqlTable(
    table: Table,
    locIdOpt: Option[LocationId] = None,
    relExprOpt: Option[RelExpr] = None
) extends SqlDbObject {
    override val name: String = table.name
}

private[scleradb]
case class SqlObjectAsExpr(
    override val name: String,
    objExpr: RelExpr,
    objectStatus: DbObjectStatus
) extends SqlDbObject

private[scleradb]
sealed abstract class SqlMLObject extends SqlDbObject {
    override val name: String
    val specOpt: Option[(String, String)]
    val numDistinctValuesMap: Map[ColRef, Int]
}

private[scleradb]
case class SqlClassifier(
    override val name: String,
    override val specOpt: Option[(String, String)],
    targetCol: ColRef,
    override val numDistinctValuesMap: Map[ColRef, Int]
) extends SqlMLObject

private[scleradb]
case class SqlClusterer(
    override val name: String,
    override val specOpt: Option[(String, String)],
    override val numDistinctValuesMap: Map[ColRef, Int]
) extends SqlMLObject

private[scleradb]
sealed abstract class Format

private[scleradb]
case object ShortFormat extends Format

private[scleradb]
case object LongFormat extends Format
