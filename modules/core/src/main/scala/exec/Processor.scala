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

import org.slf4j.{Logger, LoggerFactory}

import java.sql.SQLException

import com.scleradb.config.ScleraConfig

import com.scleradb.util.tools.Counter
import com.scleradb.util.automata.datatypes.Label

import com.scleradb.plan.PlanExplain
import com.scleradb.objects._

import com.scleradb.dbms.location._
import com.scleradb.dbms.rdbms.driver.SqlDriver

import com.scleradb.sql.objects._
import com.scleradb.sql.types._
import com.scleradb.sql.datatypes._
import com.scleradb.sql.result._
import com.scleradb.sql.statements._
import com.scleradb.sql.expr._
import com.scleradb.sql.parser._
import com.scleradb.sql.plan._
import com.scleradb.sql.exec.ScalExprEvaluator

import com.scleradb.analytics.ml.objects.{SchemaMLObject, MLObjectId}
import com.scleradb.analytics.ml.classifier.objects._
import com.scleradb.analytics.ml.clusterer.objects._

import com.scleradb.external.objects.ExternalTarget

class Processor(
    schemaDbms: String,
    val schemaDb: String,
    schemaDbConfig: List[(String, String)],
    tempDbms: String,
    tempDb: String,
    tempDbConfig: List[(String, String)],
    checkSchema: Boolean
) {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

    lazy val schema: Schema = new Schema(
        processor = this, schemaDbms = schemaDbms,
        schemaDb = schemaDb, schemaDbConfig = schemaDbConfig,
        tempDbms = tempDbms, tempDb = tempDb, tempDbConfig = tempDbConfig
    )

    def dataCacheLocation = Location.dataCacheLocation(schema)

    val scalExprEvaluator: ScalExprEvaluator = new ScalExprEvaluator(this)
    val parser: SqlParser =
        new SqlParser(schema, scalExprEvaluator)
        with SqlQueryParser with SqlCudParser with SqlAdminParser

    val normalizer: Normalizer = new Normalizer(schema, scalExprEvaluator)
    val planner: Planner = new Planner(this)
    val planExplain: PlanExplain =
        new PlanExplain(normalizer, planner, scalExprEvaluator)

    def init(): Unit = {
        schema.init()
        if( checkSchema ) schema.checkSchemaStore()
    }

    def locationDbHandler(locationId: LocationId): DbHandler =
        schema.dbHandlerOpt(locationId) getOrElse {
            throw new IllegalArgumentException(
                "Location \"" + locationId.repr + "\" not found"
            )
        }

    def handleStatement[T](
        qstr: String,
        f: TableResult => T
    ): T = parser.parseSqlStatements(qstr) match {
        case List(qstmt: SqlRelQueryStatement) =>
            handleQueryStatement(qstmt, f)

        case _ =>
            throw new IllegalArgumentException(
                "Expecting a single query statement, found: \"" + qstr + "\""
            )
    }

    def handleStatement[T](ustr: String): Unit =
        parser.parseSqlStatements(ustr) match {
            case List(ustmt: SqlUpdateStatement) =>
                handleUpdateStatement(ustmt)

            case List(astmt: SqlAdminStatement) =>
                handleAdminStatement(astmt)

            case _ =>
                throw new IllegalArgumentException(
                    "Expecting a single update/admin statement," +
                    " found: \"" + ustr + "\""
                )
        }

    def handleQueryStatement[T](
        qstmt: SqlRelQueryStatement,
        f: TableResult => T
    ): T =
        try {
            val SqlRelQueryStatement(expr) =
                normalizer.normalizeQueryStatement(qstmt)

            val plan: RelEvalPlan = planner.planRelEval(expr.tableView)
            plan.init()
            try f(plan.result.tableResult) finally plan.dispose()
        } catch { case (e: Throwable) =>
            logger.error(e.getStackTrace.mkString("\n"))
            throw e
        }

    def handleQuery[T](
        query: RelExpr,
        f: TableResult => T
    ): T = handleQueryStatement(SqlRelQueryStatement(query), f)

    def handleListQuery[T](
        relExpr: RelExpr,
        f: TableRow => T
    ): List[T] = handleQueryStatement(
        SqlRelQueryStatement(relExpr),
        { rs => rs.rows.map(row => f(row)).toList }
    )

    def handleSingletonQuery[T](
        relExpr: RelExpr,
        f: TableRow => T
    ): Option[T] = handleListQuery(relExpr, f).headOption

    def handleUpdateStatement(ustmt: SqlUpdateStatement): Option[Int] =
        try normalizer.normalizeUpdateStatement(ustmt) match {
            case SqlCreateExt(dataTarget, query) =>
                createExt(dataTarget, query)
                None
            case SqlCreateDbObject(sqlObject, dur) =>
                createDbObject(sqlObject, dur)
                None
            case SqlCreateMLObject(libOpt, obj, trainRelExpr, dur) =>
                createMLObject(libOpt, obj, trainRelExpr, dur)
                None
            case SqlDropById(id) =>
                drop(id)
                None
            case SqlDropExplicit(obj, duration) =>
                dropExplicit(obj, duration)
                None
            case stmt@SqlInsertValueRows(_, _, rows) =>
                updateTable(stmt)
                Some(rows.size)
            case SqlInsertQueryResult(tableId, targetCols, query) =>
                val tableRef: TableRefTarget =
                    TableRefTargetById(schema, tableId, targetCols)
                insert(tableRef, query)
                None
            case (stmt: SqlUpdateTable) =>
                updateTable(stmt)
                None
            case (stmt: SqlCreateIndex) =>
                createIndex(stmt)
                None
            case (stmt: SqlDropIndex) =>
                dropIndex(stmt)
                None
            case SqlUpdateBatch(stmts) =>
                updateBatch(stmts)
                None
            case SqlNativeStatement(locId, stmtStr) =>
                executeNativeStatement(locId, stmtStr)
                None
            case other =>
                throw new RuntimeException(
                    "Cannot process statement: " + other
                )
        } catch { case (e: Throwable) =>
            logger.error(e.getStackTrace.mkString("\n"))
            throw e
        }

    private def createExt(
        dataTarget: ExternalTarget,
        relExpr: RelExpr
    ): Unit = {
        val plan: RelEvalPlan = planner.planRelEval(relExpr)
        plan.init()

        try dataTarget.write(plan.result.tableResult) finally plan.dispose()
    }

    private def createDbObject(
        sqlDbObj: SqlDbObject,
        duration: DbObjectDuration
    ): SchemaObject = sqlDbObj match {
        case SqlTable(table, locIdOpt, relExprOpt) =>
            createTable(table, duration, locIdOpt, relExprOpt)
        case SqlObjectAsExpr(name, expr, DbVirtual) =>
            createView(name, expr, duration)
        case SqlObjectAsExpr(name, expr: RelExpr,
                             DbMaterialized(locIdOpt)) =>
            createTable(name, expr, duration, locIdOpt)
        case _ =>
            throw new RuntimeException("Cannot create: " + sqlDbObj)
    }

    private def createTable(
        table: Table,
        duration: DbObjectDuration,
        locIdOpt: Option[LocationId],
        relExprOpt: Option[RelExpr]
    ): SchemaTable = relExprOpt match {
        case None =>
            createTable(table, duration, locIdOpt)

        case Some(relExpr) =>
            val schemaTable: SchemaTable = createTable(
                table, duration, locIdOpt orElse relExpr.locationIdOpt
            )

            try insert(TableRefTargetExplicit(schema, schemaTable), relExpr)
            catch { case (e: Throwable) =>
                // drop the table created above
                dropExplicit(schemaTable, duration)
                throw e
            }

            schemaTable
    }

    def createTable(
        table: Table,
        duration: DbObjectDuration,
        locIdOpt: Option[LocationId]
    ): SchemaTable = {
        val locId: LocationId = locIdOpt getOrElse Location.defaultLocationId
        val updDuration: DbObjectDuration =
            if( locId.location(schema).isTemporary ) Temporary else duration

        val schemaTable: SchemaTable = SchemaTable(table, locId)

        try schema.addObject(schemaTable, Nil, updDuration)
        catch { case (e: Throwable) =>
            throw new IllegalArgumentException(
                "Could not store the metadata for table \"" + table.name + "\"",
                e
            )
        }

        try {
            val dbHandler: DbHandler = locationDbHandler(locId)
            val stmt: SqlCreateDbObject =
                SqlCreateDbObject(SqlTable(table, Some(locId)), updDuration)
            dbHandler.handleUpdate(stmt)
        } catch { case (e: Throwable) =>
            schema.removeObject(schemaTable.id)
            throw e
        }

        schemaTable
    }

    private def createView(
        name: String,
        expr: LogicalExpr,
        duration: DbObjectDuration
    ): SchemaView = expr match {
        case (relExpr: RelExpr) =>
            val view: View = View(name, relExpr)
            val schemaView: SchemaView = SchemaView(view)

            try schema.addObject(schemaView, Nil, duration)
            catch { case (e: Throwable) =>
                throw new IllegalArgumentException(
                    "Could not store the metadata for view \"" +
                    name + "\": " + e.getMessage(), e
                )
            }

            schemaView
        case _ =>
            throw new IllegalArgumentException(
                "Non-relational views are not supported"
            )
    }

    // execution takes care of cross-location transfer
    // in case locId != relExpr.locationId
    private def createTable(
        name: String,
        relExpr: RelExpr,
        duration: DbObjectDuration,
        locIdOpt: Option[LocationId]
    ): SchemaTable = {
        val locId: LocationId = locIdOpt getOrElse Location.defaultLocationId
        val updDuration: DbObjectDuration =
            if( locId.location(schema).isTemporary ) Temporary else duration

        val updRelExpr: RelExpr = relExpr.locationIdOpt match {
            case Some(rLocId) if locId != rLocId =>
                // result evaluated at a different location
                // needs to be evaluated and materialized at the target
                RelOpExpr(EvaluateOp, List(relExpr))
            case _ => relExpr
        }

        val plan: RelPlan = planner.planRel(updRelExpr)
        plan.init()

        try plan.result match {
            case RelExprPlanResult(preparedRelExpr) =>
                createTableFromExpr(preparedRelExpr, name, updDuration)
            case RelEvalPlanResult(tableResult) =>
                createTableFromTuples(
                    tableResult.columns, tableResult.rows,
                    locId, name, updDuration
                )
        } finally plan.dispose()
    }

    def createTableFromExpr(
        preparedRelExpr: RelExpr,
        tableName: String,
        duration: DbObjectDuration
    ): SchemaTable = {
        val locId: LocationId =
            preparedRelExpr.locationIdOpt getOrElse Location.dataCacheLocationId
        val updDuration: DbObjectDuration =
            if( locId.location(schema).isTemporary ) Temporary else duration
        val dbHandler: DbHandler = locationDbHandler(locId)

        val sqlObj: SqlObjectAsExpr =
            SqlObjectAsExpr(
                tableName, preparedRelExpr, DbMaterialized(Some(locId))
            )

        val stmt: SqlCreateDbObject = SqlCreateDbObject(sqlObj, updDuration)
        val table: Table = dbHandler.handleUpdate(stmt) getOrElse {
            dbHandler.table(tableName, Table.BaseTable)
        }

        val schemaTable: SchemaTable = SchemaTable(table, locId)

        try schema.addObject(schemaTable, Nil, duration)
        catch { case (e: Throwable) =>
            dropExplicit(schemaTable, updDuration)

            throw new IllegalArgumentException(
                "Could not store the metadata for table \"" + table.name + "\"",
                e
            )
        }

        schemaTable
    }

    def createTableFromTuples(
        dataColumns: List[Column],
        dataRows: Iterator[TableRow],
        dstLocId: LocationId,
        dstTableName: String,
        dstTableDuration: DbObjectDuration
    ): SchemaTable = {
        val dstDbHandler: DbHandler = locationDbHandler(dstLocId)
        val updDstTableDuration: DbObjectDuration =
            if( dstLocId.location(schema).isTemporary ) Temporary
            else dstTableDuration

        val table: Table = dstDbHandler.handleTransferIn(
            dataColumns, dataRows, dstTableName, updDstTableDuration
        )

        val schemaTable: SchemaTable = SchemaTable(table, dstLocId)

        try schema.addObject(schemaTable, Nil, updDstTableDuration)
        catch { case (e: Throwable) =>
            // delete the created table
            dstDbHandler.handleUpdate(
                SqlDropExplicit(schemaTable, updDstTableDuration)
            )

            throw new IllegalArgumentException(
                "Could not store the metadata for table \"" + table.name + "\"",
                e
            )
        }

        schemaTable
    }

    private def materializeTable(
        preparedRelExpr: RelExpr,
        duration: DbObjectDuration
    ): (TableRefSource, Option[TableId]) = preparedRelExpr match {
        case (tRef: TableRefSource) => (tRef, None)
        case _ =>
            val tableName: String = Counter.nextSymbol("M")
            val schemaTable: SchemaTable =
                createTableFromExpr(preparedRelExpr, tableName, duration)
            (TableRefSourceExplicit(schema, schemaTable), Some(schemaTable.id))
    }

    private def createMLObject(
        libOpt: Option[String],
        sqlMLObj: SqlMLObject,
        trainRelExpr: RelExpr,
        duration: DbObjectDuration
    ): SchemaMLObject = {
        val plan: RelEvalPlan = planner.planRelEval(trainRelExpr)
        plan.init()

        try {
            val rs: TableResult = plan.result.tableResult

            val schemaMLObject: SchemaMLObject = sqlMLObj match {
                case SqlClassifier(name, specOpt,
                                   targetCol, numDistinctValuesMap) =>
                    val trainCols: List[ColRef] = trainRelExpr.tableColRefs
                    val targetColIndex: Int = trainCols.indexOf(targetCol)

                    val classifier: Classifier =
                        Classifier(
                            libOpt, name, specOpt,
                            targetColIndex, numDistinctValuesMap, rs
                        )

                    SchemaClassifier(classifier)

                case SqlClusterer(name, specOpt, numDistinctValuesMap) =>
                    val clusterer: Clusterer =
                        Clusterer(
                            libOpt, name, specOpt, numDistinctValuesMap, rs
                        )

                    SchemaClusterer(clusterer)
            }

            // TODO: Handle errors
            schema.addObject(schemaMLObject, Nil, duration)

            schemaMLObject
        } finally plan.dispose()
    }

    def drop(id: SchemaObjectId): Unit = drop(id.repr)

    private def drop(id: String): Unit = {
        val obj: SchemaObject = schema.objectOpt(id) getOrElse {
            throw new IllegalArgumentException(
                "Object \"" + id + "\" not found"
            )
        }

        val duration: DbObjectDuration = SchemaObject.duration(schema, id)
        dropExplicit(obj, duration)
    }

    private def dropExplicit(
        obj: SchemaObject,
        duration: DbObjectDuration
    ): Unit = {
        val deps: List[String] = schema.dependencies(obj.id.repr)

        obj match {
            case (st: SchemaTable) =>
                locationDbHandler(st.locationId).handleUpdate(
                    SqlDropExplicit(st, duration)
                )
            case _ => () // nothing needs to be dropped at the location
        }

        schema.removeObject(obj.id)

        deps.distinct.reverse.foreach { dep => drop(dep) }
    }

    private def insert(tableRef: TableRefTarget, relExpr: RelExpr): Unit = {
        val updRelExpr: RelExpr = relExpr.locationIdOpt match {
            case Some(rLocId) if tableRef.locationId != rLocId =>
                // result evaluated at a different location
                // needs to be evaluated and materialized at the target
                RelOpExpr(EvaluateOp, List(relExpr))
            case _ => relExpr
        }

        val plan: RelPlan = planner.planRel(updRelExpr)
        plan.init()

        try plan.result match {
            case RelExprPlanResult(preparedRelExpr) =>
                val nCols: Int = preparedRelExpr.tableColRefs.size
                updateTable(
                    SqlInsert(
                        tableRef.tableId,
                        tableRef.tableColRefs.take(nCols),
                        preparedRelExpr
                    )
                )
            case RelEvalPlanResult(tableResult) =>
                insert(tableRef, tableResult.columns, tableResult.rows)
        } finally plan.dispose()
    }

    def insert(
        tableRef: TableRefTarget,
        dataColumns: List[Column],
        dataRows: Iterator[TableRow]
    ): Unit = {
        val dbHandler: DbHandler = locationDbHandler(tableRef.locationId)
        dbHandler.handleInsertIn(dataColumns, dataRows, tableRef)
    }

    def delete(tableId: TableId, predExpr: ScalExpr = BoolConst(true)): Unit =
        handleUpdateStatement(SqlDelete(tableId, predExpr))

    private def updateTable(stmt: SqlUpdateTable): Unit = {
        val locationId: LocationId = stmt.tableId.locationId
        locationDbHandler(locationId).handleUpdate(stmt)
    }

    def createIndex(
        stmt: SqlCreateIndex,
        dur: DbObjectDuration = Persistent
    ): Unit = {
        val locationId: LocationId = stmt.tableId.locationId
        val updDur: DbObjectDuration =
            SchemaObject.duration(schema, stmt.tableId) match {
                case Temporary => Temporary // underlying relation is temporary
                case Persistent => dur
            }

        locationDbHandler(locationId).handleUpdate(stmt)
        schema.addIndexLocation(stmt.name, locationId, updDur)
    }

    def dropIndex(stmt: SqlDropIndex): Unit = {
        val locationId: LocationId =
            schema.indexLocationOpt(stmt.indexName) getOrElse {
                throw new IllegalArgumentException(
                    "Index \"" + stmt.indexName + "\" not found"
                )
            }

        locationDbHandler(locationId).handleUpdate(stmt)
        schema.removeIndexLocation(stmt.indexName)
    }

    private def updateBatch(stmts: List[SqlUpdateTable]): Unit = {
        // group consecutive statements by location
        val stmtGroups: List[(LocationId, List[SqlUpdateTable])] =
            stmts.foldLeft (List[(LocationId, List[SqlUpdateTable])]()) {
                case ((lastLocId, lastStmts)::rest, stmt)
                if( stmt.tableId.locationId == lastLocId ) =>
                    // same location as earlier statement
                    // add to the last group
                    (lastLocId, stmt::lastStmts)::rest

                case (prev, stmt) =>
                    // first statement, or different location from earlier stmt
                    // start a new group
                    (stmt.tableId.locationId, List(stmt))::prev
            }

        // execute each group at the respective location
        // note that all lists need to be reversed
        stmtGroups.reverse.foreach {
            case (locId, locStmts) =>
                val locBatchStmt: SqlUpdateBatch =
                    SqlUpdateBatch(locStmts.reverse)
                locationDbHandler(locId).handleUpdate(locBatchStmt)
        }
    }

    private def executeNativeStatement(
        locId: LocationId,
        stmtStr: String
    ): Unit = {
        val dbHandler: DbHandler = locationDbHandler(locId)
        dbHandler.executeNativeStatement(stmtStr)
    }

    def queryResult(preparedRelExpr: RelExpr): TableResult = {
        val locationId: LocationId = preparedRelExpr.locationIdOpt getOrElse {
            throw new RuntimeException(
                "Expected that the result will be computed at location: " +
                preparedRelExpr
            )
        }

        val dbHandler: DbHandler = locationDbHandler(locationId)
        dbHandler.queryResult(preparedRelExpr)
    }

    def handleAdminStatement(astmt: SqlAdminStatement): Unit =
        try astmt match {
            case SqlCreateSchema =>
                schema.createSchema()
            case SqlDropSchema =>
                schema.dropSchema()
            case SqlAddLocation(id, dbName, params, dbSchemaOpt, permStrOpt) =>
                val loc: Location = Location(
                    schema, id, dbName, params, dbSchemaOpt,
                    LocationPermit(permStrOpt)
                )
                schema.addLocation(loc)
            case SqlRemoveLocation(locId) =>
                schema.removeLocation(locId)
            case SqlAddTable(tableId, tableOpt) =>
                addTable(tableId, tableOpt)
            case SqlRemoveTable(tableId) =>
                removeTable(tableId)
            case SqlExplainScript(isExplainVal) =>
                ScleraConfig.setExplain(isExplainVal)
            case SqlConfigLocation(param, locId) =>
                Location.configLocation(param, locId)
        } catch { case (e: Throwable) =>
            logger.error(e.getStackTrace.mkString("\n"))
            throw e
        }

    def addTable(tableId: TableId, tableOpt: Option[Table]): Unit = {
        val locationId: LocationId = tableId.locationId
        val (baseTable, duration) =
            locationDbHandler(locationId).table(tableId.name)
        val table: Table = tableOpt getOrElse baseTable

        val schemaTable: SchemaTable = SchemaTable(table, locationId)
        try schema.addObject(schemaTable, Nil, duration)
        catch { case (e: Throwable) =>
            throw new IllegalArgumentException(
                "Could not store the metadata for table \"" + table.name + "\"",
                e
            )
        }
    }

    def removeTable(tableId: TableId) =
        schema.removeObject(tableId)

    def handleAdminQueryStatement(
        aqstmt: SqlAdminQueryStatement
    ): TableResult =
        try aqstmt match {
            case SqlListRemainingTables(locIdOpt, format) =>
                val tableInfo: List[(SchemaTable, DbObjectDuration)] =
                    remainingTables(locIdOpt)

                listObjects(tableInfo, format)
            case SqlListAddedTables(locIdOpt, nameOpt, format) =>
                val allTables: List[SchemaTable] =
                    SchemaTable.objects(
                        schema, locIdOpt, nameOpt
                    )
                val tables: List[SchemaTable] =
                    allTables diff schema.schemaTables

                listObjects(
                    tables.map { t =>
                        (t, SchemaObject.duration(schema, t.id))
                    },
                    format
                )
            case SqlListViews(nameOpt, format) =>
                val views: List[SchemaView] = nameOpt match {
                    case Some(name) =>
                        SchemaView.objectOpt(
                            schema, ViewId(name)
                        ).toList
                    case None =>
                        SchemaView.objects(schema)
                }

                listObjects(
                    views.map { v =>
                        (v, SchemaObject.duration(schema, v.id))
                    },
                    format
                )
            case SqlListClassifiers(nameOpt, format) =>
                val cs: List[SchemaClassifier] = nameOpt match {
                    case Some(name) =>
                        SchemaClassifier.objectOpt(
                            schema, MLObjectId(name)
                        ).toList
                    case None =>
                        SchemaClassifier.objects(schema)
                }

                listObjects(
                    cs.map { c =>
                        (c, SchemaObject.duration(schema, c.id))
                    },
                    format
                )
            case SqlListClusterers(nameOpt, format) =>
                val cs: List[SchemaClusterer] = nameOpt match {
                    case Some(name) =>
                        SchemaClusterer.objectOpt(
                            schema, MLObjectId(name)
                        ).toList
                    case None =>
                        SchemaClusterer.objects(schema)
                }

                listObjects(
                    cs.map { c =>
                        (c, SchemaObject.duration(schema, c.id))
                    },
                    format
                )
            case SqlListObjects(nameOpt, format) =>
                val allObjs: List[SchemaObject] = nameOpt match {
                    case Some(name) =>
                        SchemaTable.objectsByName(schema, name):::
                        SchemaView.objectsByName(schema, name):::
                        SchemaMLObject.objectsByName(schema, name)

                    case None => SchemaObject.objects(schema)
                }
                val objs: List[SchemaObject] =
                    allObjs diff schema.schemaTables

                listObjects(
                    objs.map { x =>
                        (x, SchemaObject.duration(schema, x.id))
                    },
                    format
                )
            case SqlListLocations =>
                listLocations
            case SqlExplainPlan(relExpr) =>
                explain(relExpr)
            case SqlShowConfig =>
                showConfig
            case SqlShowOptions =>
                showOptions
        } catch { case (e: Throwable) =>
            logger.error(e.getStackTrace.mkString("\n"))
            throw e
        }

    def remainingTables(
        locIdOpt: Option[LocationId]
    ): List[(SchemaTable, DbObjectDuration)] =
        baseTables(locIdOpt).filter { case (st, _) =>
            !SchemaTable.objectOpt(schema, st.id).isDefined
        }

    def baseTables(
        locIdOpt: Option[LocationId]
    ): List[(SchemaTable, DbObjectDuration)] = {
        val locIds: List[LocationId] = locIdOpt match {
            case Some(locId) => List(locId)
            case None => schema.locationIds
        }

        locIds.flatMap { locId =>
            locationDbHandler(locId).tables.map {
                case (t, dur) => (SchemaTable(t, locId), dur)
            }
        }
    }

    private def listObjects(
        dbObjects: List[(SchemaObject, DbObjectDuration)],
        format: Format
    ): TableResult = {
        val sortedDbObjects: List[(SchemaObject, DbObjectDuration)] =
            dbObjects.sortBy { case (t, _) => t.id.name }

        format match {
            case ShortFormat =>
                val cols: List[Column] =
                    List("TYPE", "NAME", "LOCATION", "DURATION").map {
                        colName => Column(colName, SqlCharVarying(None))
                    }

                val descs: Iterator[Map[String, CharConst]] =
                    sortedDbObjects.iterator.map { case (obj, duration) =>
                        Map(
                            "TYPE" -> CharConst(obj.typeStr),
                            "NAME" -> CharConst(obj.name),
                            "LOCATION" -> CharConst(
                                obj.locationIdOpt match {
                                    case Some(locId)
                                    if( !Location.isActive(schema, locId) ) =>
                                        locId.repr + " [UNREACHABLE]"
                                    case Some(locId) => locId.repr
                                    case None => "SCLERA"
                                }
                            ),
                            "DURATION" -> CharConst(
                                duration match {
                                    case Temporary => "TEMPORARY"
                                    case Persistent => "PERSISTENT"
                                }
                            )
                        )
                    }

                ScalTableResult(cols, descs)

            case LongFormat =>
                val cols: List[Column] =
                    List(Column("DESCRIPTION", SqlCharVarying(None)))

                val descs: Iterator[Map[String, CharConst]] =
                    sortedDbObjects.iterator.flatMap { case (obj, dur) =>
                        val strs: List[String] =
                            DescribeObject.describe(obj, dur).flatMap { s =>
                                s.split("""\r?\n""").toList
                            }

                        strs.iterator.map { s =>
                            Map("DESCRIPTION" -> CharConst(s))
                        }
                    }

                ScalTableResult(cols, descs)
        }
    }

    private def listLocations: TableResult = {
        val cols: List[Column] = List(
            "LOCATION", "SYSTEM", "DATABASE", "PROPERTY", "REACHABLE"
        ).map { colName => Column(colName, SqlCharVarying(None)) }

        val locations: List[Location] =
            Location.allLocations(schema).sortBy { loc => loc.id.repr }

        val vals: Iterator[Map[String, CharConst]] =
            locations.iterator.map { loc =>
                Map(
                    "LOCATION" -> CharConst(loc.id.repr),
                    "SYSTEM" -> CharConst(loc.dbms),
                    "DATABASE" -> CharConst(
                        loc.param +
                        loc.dbSchemaOpt.map(s => ":" + s).getOrElse("")
                    ),
                    "PROPERTY" -> CharConst(loc.permit.repr),
                    "REACHABLE" -> CharConst(
                        if( Location.isActive(schema, loc.id) ) "YES"
                        else "NO"
                    )
                )
            }

        ScalTableResult(cols, vals)
    }

    private def explain(relExpr: RelExpr): TableResult = ScalTableResult(
        List(Column("EXPLAIN", SqlCharVarying(None))),
        planExplain.explain(relExpr).iterator.map { s =>
            Map("EXPLAIN" -> CharConst(s))
        }
    )

    private def showConfig: TableResult = {
        val cols: List[Column] = 
            List("CONFIG", "VALUE").map {
                colName => Column(colName, SqlCharVarying(None))
            }

        val vals: Iterator[Map[String, CharConst]] =
            ScleraConfig.configValPairs.iterator.map { case (k, v) =>
                Map("CONFIG" -> CharConst(k), "VALUE" -> CharConst(v))
            }

        ScalTableResult(cols, vals)
    }

    private def showOptions: TableResult = {
        val cols: List[Column] = 
            List("OPTION", "VALUE").map {
                colName => Column(colName, SqlCharVarying(None))
            }

        val vals: Iterator[Map[String, CharConst]] = Iterator(
            Map(
                "OPTION" -> CharConst("Temporary Location"),
                "VALUE" -> CharConst(Location.tempLocationId.repr)
            ),
            Map(
                "OPTION" -> CharConst("Default Location"),
                "VALUE" -> CharConst(
                    try Location.defaultLocationId.repr catch {
                        case (_: Throwable) => "[NOT SET]"
                    }
                )
            ),
            Map(
                "OPTION" -> CharConst("Cache Location"),
                "VALUE" -> CharConst(Location.dataCacheLocationId.repr)
            )
        )

        ScalTableResult(cols, vals)
    }

    def close(): Unit = {
        dropTemporaryObjects()
        schema.close()
        SqlDriver.closeConnectionPools()
    }

    def dropTemporaryObjects(): Unit =
        schema.temporaryObjects.foreach { obj => dropExplicit(obj, Temporary) }

    // for JDBC
    def planQuery(
        s: SqlRelQueryStatement,
        maxRowsOpt: Option[Int]
    ): RelEvalPlan = planQuery(s.relExpr, maxRowsOpt)

    def planQuery(
        expr: RelExpr,
        maxRowsOpt: Option[Int] = None
    ): RelEvalPlan = {
        val relExpr = maxRowsOpt match {
            case None => expr
            case Some(maxRows) => expr.limit(maxRows)
        }

        val normalizedRelExpr = normalizer.normalizeRelExpr(relExpr)
        planner.planRelEval(normalizedRelExpr)
    }
}

object Processor {
    def apply(
        schemaDbms: String = ScleraConfig.schemaDbms,
        schemaDb: String = ScleraConfig.schemaDb,
        schemaDbConfig: List[(String, String)] = ScleraConfig.schemaDbConfig,
        tempDbms: String = ScleraConfig.tempDbms,
        tempDb: String = ScleraConfig.tempDb,
        tempDbConfig: List[(String, String)] = ScleraConfig.tempDbConfig,
        checkSchema: Boolean = true
    ): Processor = new Processor(
        schemaDbms = schemaDbms,
        schemaDb = schemaDb,
        schemaDbConfig = schemaDbConfig,
        tempDbms = tempDbms,
        tempDb = tempDb,
        tempDbConfig = tempDbConfig,
        checkSchema = checkSchema
    )

    def apply(info: java.util.Properties): Processor = {
        val schemaDbms: String =
            info.getProperty("schemaDbms", ScleraConfig.schemaDbms)
        val schemaDb: String =
            info.getProperty("schemaDb", ScleraConfig.schemaDb)
        val schemaDbConfig: List[(String, String)] =
            Option(info.getProperty("schemaDbConfig")).map { css =>
                css.split(";").toList.map { cs =>
                    cs.split("=") match {
                        case Array(k, v) => (k, v)
                        case _ =>
                            throw new SQLException(
                                "Invalid schema configuration: " + cs, "08000"
                            )
                    }
                }
            } getOrElse ScleraConfig.schemaDbConfig
        val tempDbms: String =
            info.getProperty("tempDbms", ScleraConfig.tempDbms)
        val tempDb: String =
            info.getProperty("tempDb", ScleraConfig.tempDb)
        val tempDbConfig: List[(String, String)] =
            Option(info.getProperty("tempDbConfig")).map { css =>
                css.split(";").toList.map { cs =>
                    cs.split("=") match {
                        case Array(k, v) => (k, v)
                        case _ =>
                            throw new SQLException(
                                "Invalid temp configuration: " + cs, "08000"
                            )
                    }
                }
            } getOrElse ScleraConfig.tempDbConfig

        val checkSchema: Boolean =
            Option(info.getProperty("checkSchema")).flatMap { boolStr =>
                boolStr.toBooleanOption
            } getOrElse true

        apply(
            schemaDbms = schemaDbms,
            schemaDb = schemaDb,
            schemaDbConfig = schemaDbConfig,
            tempDbms = tempDbms,
            tempDb = tempDb,
            tempDbConfig = tempDbConfig,
            checkSchema = checkSchema
        )
    }
}
