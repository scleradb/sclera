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
abstract class SqlStatement

abstract class SqlQueryStatement extends SqlStatement

// SQL query parsed into a relational expression
case class SqlRelQueryStatement(
    relExpr: RelExpr
) extends SqlQueryStatement {
    def resultOrder: List[SortExpr] = relExpr.resultOrder
}

// update statements
abstract class SqlUpdateStatement extends SqlStatement

// SQL/EXT CREATE/DROP variants
abstract class SqlUpdateSchema extends SqlUpdateStatement

case class SqlCreateDbSchema(dbSchema: String) extends SqlUpdateSchema

abstract class SqlCreate extends SqlUpdateSchema

case class SqlCreateExt(
    source: ExternalTarget,
    relExpr: RelExpr
) extends SqlCreate

abstract class SqlCreateObj extends SqlCreate {
    val duration: DbObjectDuration
}

case class SqlCreateDbObject(
    obj: SqlDbObject,
    override val duration: DbObjectDuration
) extends SqlCreateObj

case class SqlCreateMLObject(
    libOpt: Option[String],
    obj: SqlMLObject,
    trainRelExpr: RelExpr,
    override val duration: DbObjectDuration
) extends SqlCreateObj

abstract class SqlDrop extends SqlUpdateSchema {
    val objectId: SchemaObjectId
}

case class SqlDropById(
    override val objectId: SchemaObjectId
) extends SqlDrop

case class SqlDropExplicit(
    obj: SchemaObject,
    duration: DbObjectDuration
) extends SqlDrop {
    override val objectId: SchemaObjectId = obj.id
}

abstract class SqlUpdateTable extends SqlUpdateStatement {
    val tableId: TableId
}

abstract class SqlInsert extends SqlUpdateTable {
    val targetCols: List[ColRef]
}

case class SqlInsertQueryResult(
    override val tableId: TableId,
    override val targetCols: List[ColRef],
    query: RelExpr
) extends SqlInsert

case class SqlInsertValueRows(
    override val tableId: TableId,
    override val targetCols: List[ColRef],
    rows: List[Row]
) extends SqlInsert

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

case class SqlUpdate(
    override val tableId: TableId,
    colValPairs: List[(ColRef, ScalExpr)],
    pred: ScalExpr
) extends SqlUpdateTable

case class SqlDelete(
    override val tableId: TableId,
    pred: ScalExpr
) extends SqlUpdateTable

case class SqlUpdateBatch(
    stmts: List[SqlUpdateTable]
) extends SqlUpdateStatement

abstract class SqlIndex extends SqlUpdateStatement

case class SqlCreateIndex(
    name: String,
    tableId: TableId,
    indexCols: List[ColRef],
    pred: ScalExpr
) extends SqlIndex

case class SqlDropIndex(indexName: String) extends SqlIndex

abstract class SqlAlter extends SqlUpdateStatement

// native statement execution
case class SqlNativeStatement(
    locationId: LocationId,
    stmtStr: String
) extends SqlUpdateStatement

abstract class SqlAdminStatement extends SqlStatement

case object SqlCreateSchema extends SqlAdminStatement

case object SqlDropSchema extends SqlAdminStatement

case class SqlAddLocation(
    locationId: LocationId,
    dbname: String,
    params: List[String],
    dbSchemaOpt: Option[String],
    permitStrOpt: Option[String]
) extends SqlAdminStatement

case class SqlRemoveLocation(locationId: LocationId) extends SqlAdminStatement

case class SqlAddTable(
    tableId: TableId,
    tableOpt: Option[Table]
) extends SqlAdminStatement

case class SqlRemoveTable(tableId: TableId) extends SqlAdminStatement

case class SqlExplainScript(isExplain: Boolean) extends SqlAdminStatement

case class SqlConfigLocation(
    param: String,
    locationId: LocationId
) extends SqlAdminStatement

abstract class SqlAdminQueryStatement extends SqlStatement

case object SqlShowOptions extends SqlAdminQueryStatement

case object SqlShowConfig extends SqlAdminQueryStatement

case class SqlListRemainingTables(
    locIdOpt: Option[LocationId],
    format: Format
) extends SqlAdminQueryStatement

case class SqlListAddedTables(
    locIdOpt: Option[LocationId],
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

case class SqlListViews(
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

case class SqlListClassifiers(
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

case class SqlListClusterers(
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

case class SqlListObjects(
    nameOpt: Option[String],
    format: Format
) extends SqlAdminQueryStatement

case object SqlListLocations extends SqlAdminQueryStatement

case class SqlExplainPlan(relExpr: RelExpr) extends SqlAdminQueryStatement

sealed abstract class SqlDbObject {
    val name: String
}

case class SqlTable(
    table: Table,
    locIdOpt: Option[LocationId] = None,
    relExprOpt: Option[RelExpr] = None
) extends SqlDbObject {
    override val name: String = table.name
}

case class SqlObjectAsExpr(
    override val name: String,
    objExpr: RelExpr,
    objectStatus: DbObjectStatus
) extends SqlDbObject

sealed abstract class SqlMLObject extends SqlDbObject {
    override val name: String
    val specOpt: Option[(String, String)]
    val numDistinctValuesMap: Map[ColRef, Int]
}

case class SqlClassifier(
    override val name: String,
    override val specOpt: Option[(String, String)],
    targetCol: ColRef,
    override val numDistinctValuesMap: Map[ColRef, Int]
) extends SqlMLObject

case class SqlClusterer(
    override val name: String,
    override val specOpt: Option[(String, String)],
    override val numDistinctValuesMap: Map[ColRef, Int]
) extends SqlMLObject

sealed abstract class Format

case object ShortFormat extends Format

case object LongFormat extends Format
