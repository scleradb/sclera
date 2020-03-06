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
import java.sql.{SQLException, SQLWarning}

import scala.collection.concurrent.TrieMap

import com.scleradb.dbms.location._
import com.scleradb.objects._

import com.scleradb.sql.objects._
import com.scleradb.sql.expr._
import com.scleradb.sql.result._
import com.scleradb.sql.datatypes._

import com.scleradb.analytics.ml.objects._

import com.scleradb.config.ScleraConfig

import com.scleradb.analytics.ml.objects.SchemaMLObject

private[scleradb]
class Schema(
    processor: Processor,
    schemaDbms: String,
    schemaDb: String,
    schemaDbConfig: List[(String, String)],
    tempDbms: String,
    tempDb: String,
    tempDbConfig: List[(String, String)]
) {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

    // persistent schema location
    private val schemaLocation: Location = {
        try Location(
            this, Location.schemaLocationId,
            schemaDbms, schemaDb, None, schemaDbConfig, ReadWriteLocation
        ) catch { case (e: Throwable) =>
            throw new IllegalArgumentException("Unable to configure schema", e)
        }
    }

    private val schemaDbHandler: DbHandler =
        try DbHandler(schemaLocation) catch { case (e: Throwable) =>
            throw new IllegalArgumentException("Unable to connect to schema", e)
        }

    // persistent schema storage handler
    private val schemaStore: SchemaStore =
        new SchemaStore(processor, schemaDbHandler)

    // database location handlers
    private val dbHandlerCache: TrieMap[String, DbHandler] =
        TrieMap(schemaLocation.id.repr -> schemaDbHandler)

    // temporary index locations
    private val tempIndexLocations: TrieMap[String, LocationId] = TrieMap()

    // temporary database objects and their dependencies
    private val tempObjects: TrieMap[String, (SchemaObject, List[String])] =
        TrieMap()

    val schemaTables: List[SchemaTable] = schemaStore.schemaTables

    def init(): Unit = {
        schemaDbHandler.createDbSchema()

        // create and add a temporary main-memory location
        val tempLocation: Location = Location(
            this, Location.tempLocationId,
            tempDbms, tempDb, None, tempDbConfig, ReadWriteLocation
        )

        val tempDbHandler: DbHandler = DbHandler(tempLocation)
        tempDbHandler.createDbSchema()

        dbHandlerCache += (tempLocation.id.repr -> tempDbHandler)
    }

    def checkSchemaStore(): Unit = {
        // check schema store version
        val versionOpt: Option[String] =
            try schemaStore.versionOpt catch { case (e: SQLException) =>
                val msg: String =
                    "Schema tables are either outdated, or do not exist, at " +
                    "location \"" + Location.schemaLocationId.repr + "\". " +
                    "Please drop the current schema tables (if any) using " +
                    "the command \"drop schema\", and create " +
                    "new schema tables using the command \"create schema\"."

                logger.info(msg)
                throw new SQLWarning(msg, "55000", e)
            }

        versionOpt match {
            case Some(version) =>
                if( version != ScleraConfig.schemaVersion ) {
                    val msg: String =
                        "Schema tables at the location \"" +
                        Location.schemaLocationId.repr +
                        "\" (version = " + version + ") may not be " +
                        "consistent with the current version (" +
                        ScleraConfig.schemaVersion + "). " +
                        "Please drop the current schema tables using " +
                        "the command \"drop schema\", and create " +
                        "new schema tables using the command \"create schema\"."

                    logger.info(msg)
                    throw new SQLWarning(msg, "55000")
                }
            case None =>
                val msg: String =
                    "Cannot identify the schema table version at location \"" +
                    Location.schemaLocationId.repr + "\". " +
                    "Please drop the current schema tables using " +
                    "the command \"drop schema\", and create " +
                    "new schema tables using the command \"create schema\"."

                logger.info(msg)
                throw new SQLWarning(msg, "55000")
        }
    }

    def addLocation(loc: Location): Unit = {
        if( locationOpt(loc.id) != None )
            throw new IllegalArgumentException(
                "Location with id \"" + loc.id.repr + "\" already present"
            )

        val dbHandler: DbHandler = DbHandler(loc)

        if( !loc.isTemporary )
            schemaStore.addLocation(loc)

        dbHandlerCache += (loc.id.repr -> dbHandler)
    }

    def addObject(
        obj: SchemaObject,
        depIds: List[SchemaObjectId],
        dur: DbObjectDuration
    ): Unit = {
        if( isObjectIdPresent(obj.id.repr) )
            throw new IllegalArgumentException(
                "Object with id \"" + obj.id.repr + "\" already present"
            )

        // if the target is a temporary location, override duration to Temporary
        val updDur: DbObjectDuration = obj match {
            case (st: SchemaTable) =>
                val locId: LocationId = st.id.locationId
                val loc: Location = locationOpt(locId) getOrElse {
                    throw new IllegalArgumentException(
                        "Location \"" + locId.repr + "\" not found"
                    )
                }

                if( loc.isTemporary ) Temporary else dur

            case _ => dur
        }

        updDur match {
            case Temporary =>
                val deps: List[String] = depIds.map { id => id.repr }
                tempObjects += (obj.id.repr -> (obj, deps))

            case Persistent =>
                schemaStore.addObject(obj, depIds)
        }
    }

    def removeLocation(id: LocationId): Unit = {
        val idStr: String = id.repr

        if( Location.isDefault(id) ) throw new IllegalArgumentException(
            "Cannot remove default location: " + idStr
        )

        if( Location.isCache(id) ) throw new IllegalArgumentException(
            "Cannot remove cache location: " + idStr
        )

        val locObjs: List[SchemaObject] =
            tempObjects.values.toList.flatMap {
                case (st: SchemaTable, _)
                if( st.locationId.repr == idStr ) => Some(st)
                case _ => None
            }

        if( !locObjs.isEmpty ) {
            val locObjStrs: List[String] =
                locObjs.map { obj => obj.id.repr }
            throw new IllegalArgumentException(
                "Cannot remove location \"" + idStr + "\" as " +
                "it contains objects [" + locObjStrs.mkString(", ") + "]"
            )
        }

        val locIndexes: List[(String, LocationId)] = indexLocations.filter {
            case (_, locId) => (locId.repr == idStr)
        }

        if( !locIndexes.isEmpty ) {
            val locIndexStrs: List[String] =
                locIndexes.map { case (i, _) => i }
            throw new IllegalArgumentException(
                "Cannot remove location \"" + idStr + "\" as " +
                "it contains indexes [" + locIndexStrs.mkString(", ") + "]"
            )
        }

        schemaStore.removeLocation(idStr)

        dbHandlerCache.get(idStr).foreach { dbHandler => dbHandler.close() }
        dbHandlerCache -= idStr
    }

    def removeObject(id: SchemaObjectId): Unit = {
        val idStr: String = id.repr
        if( tempObjects contains idStr )
            tempObjects -= idStr
        else schemaStore.removeObject(id)
    }

    // are all the locations loaded in the cache?
    private var isDbHandlerCacheComplete: Boolean = false
    private def updateDbHandlerCache(): Unit = if( !isDbHandlerCacheComplete ) {
        val activeLocIds: List[String] = dbHandlerCache.keys.toList

        schemaStore.remLocations(activeLocIds).foreach { loc =>
            DbHandler.get(loc).foreach { dbh =>
                dbHandlerCache += (loc.id.repr -> dbh)
            }
        }

        isDbHandlerCacheComplete = true
    }

    def dbHandlers: List[DbHandler] = {
        updateDbHandlerCache()
        dbHandlerCache.values.toList
    }

    def dbHandlerOpt(id: String): Option[DbHandler] =
        dbHandlerCache.get(id) orElse {
            schemaStore.locationOpt(id).map { loc =>
                // add to the cache as well
                val dbh: DbHandler = DbHandler(loc)
                dbHandlerCache += (loc.id.repr -> dbh)

                dbh
            }
        }

    def dbHandlerOpt(id: LocationId): Option[DbHandler] = dbHandlerOpt(id.repr)

    def locationIds: List[LocationId] = {
        val activeLocIdStrs: List[String] = dbHandlerCache.keys.toList

        val remLocationIds: List[LocationId] =
            schemaStore.remLocationIds(activeLocIdStrs)
        if( remLocationIds.isEmpty )
            isDbHandlerCacheComplete = true

        val allLocIds: List[LocationId] =
            remLocationIds :::
            dbHandlerCache.values.toList.map { dbh => dbh.location.id }

        allLocIds.filter { id => !Location.isReserved(id) }
    }

    def allLocations: List[Location] = {
        updateDbHandlerCache()
        val activeLocations: List[Location] =
            dbHandlerCache.values.toList.map { dbh => dbh.location }
        val inactiveLocations: List[Location] =
            schemaStore.remLocations(dbHandlerCache.keys.toList)
        activeLocations:::inactiveLocations
    }

    def isActiveLocation(id: LocationId): Boolean = {
        updateDbHandlerCache()
        dbHandlerCache.contains(id.repr)
    }

    def locations: List[Location] = {
        updateDbHandlerCache()
        dbHandlerCache.values.toList.map { dbh => dbh.location } filter { loc =>
            !Location.isReserved(loc.id)
        }
    }

    def locationOpt(id: String): Option[Location] =
        dbHandlerOpt(id).map { dbh => dbh.location }

    def locationOpt(id: LocationId): Option[Location] = locationOpt(id.repr)

    def persistentObjects: List[SchemaObject] =
        schemaStore.tables:::schemaStore.views:::schemaStore.mlObjects
        
    def objects: List[SchemaObject] =
        tempObjects.values.toList.map { case (obj, _) => obj } :::
        persistentObjects

    def tables: List[SchemaTable] =
        tempObjects.values.toList.flatMap {
            case (obj: SchemaTable, _) => Some(obj)
            case _ => None
        } ::: schemaStore.tables
    
    def tables(locIds: List[LocationId]): List[SchemaTable] =
        tempObjects.values.toList.flatMap {
            case (obj: SchemaTable, _)
            if locIds contains obj.locationId => Some(obj)
            case _ => None
        } ::: schemaStore.tables(locIds)

    def views: List[SchemaView] =
        tempObjects.values.toList.flatMap {
            case (obj: SchemaView, _) => Some(obj)
            case _ => None
        } ::: schemaStore.views

    def mlObjects: List[SchemaMLObject] =
        tempObjects.values.toList.flatMap {
            case (obj: SchemaMLObject, _) => Some(obj)
            case _ => None
        } ::: schemaStore.mlObjects
    
    def objectOpt(id: String): Option[SchemaObject] =
        tableOpt(id) orElse viewOpt(id) orElse mlObjectOpt(id)

    def tableOpt(id: String): Option[SchemaTable] =
        tempObjects.get(id) match {
            case Some((obj: SchemaTable, _)) => Some(obj)
            case _ => schemaStore.tableOpt(id)
        }
    
    def viewOpt(id: String): Option[SchemaView] =
        tempObjects.get(id) match {
            case Some((obj: SchemaView, _)) => Some(obj)
            case _ => schemaStore.viewOpt(id)
        }

    def mlObjectOpt(id: String): Option[SchemaMLObject] =
        tempObjects.get(id) match {
            case Some((obj: SchemaMLObject, _)) => Some(obj)
            case _ => schemaStore.mlObjectOpt(id)
        }
    
    def isTemporary(id: String): Boolean = tempObjects contains id

    def duration(id: String): DbObjectDuration =
        if( isTemporary(id) ) Temporary else Persistent

    def dependencies(id: String): List[String] =
        tempObjects.get(id).map {
            case (_, deps) => deps
        } getOrElse schemaStore.dependencies(id)

    def temporaryObjects: List[SchemaObject] =
        tempObjects.values.toList.map { case (obj, _) => obj }

    def isObjectIdPresent(id: String): Boolean =
        (tempObjects contains id) || schemaStore.isObjectIdPresent(id)

    def addIndexLocation(
        indexName: String,
        locationId: LocationId,
        dur: DbObjectDuration
    ): Unit = {
        val loc: Location = locationOpt(locationId) getOrElse {
            throw new IllegalArgumentException(
                "Location \"" + locationId.repr + "\" not found"
            )
        }

        val updDur: DbObjectDuration = if( loc.isTemporary ) Temporary else dur
        updDur match {
            case Temporary =>
                tempIndexLocations += (indexName -> locationId)
            case Persistent =>
                schemaStore.addIndexLocation(indexName, locationId)
        }
    }

    def indexLocations: List[(String, LocationId)] =
        tempIndexLocations.toList:::schemaStore.indexLocations

    def indexLocationOpt(indexName: String): Option[LocationId] =
        tempIndexLocations.get(indexName) orElse
        schemaStore.indexLocationOpt(indexName)

    def removeIndexLocation(indexName: String): Unit =
        if( tempIndexLocations contains indexName )
            tempIndexLocations -= indexName
        else schemaStore.removeIndexLocation(indexName)

    def close(): Unit = {
        dbHandlerCache.values.foreach { dbh =>
            try dbh.close() catch { case (e: Throwable) =>
                println(dbh.location.id.repr + ": " + e.getMessage())
            }
        }

        dbHandlerCache.clear()
    }

    def createSchema(): Unit = schemaStore.createSchemaTables()

    def dropSchema(): Unit =
        try schemaStore.dropSchemaObjects() catch { case (e: SQLException) =>
            logger.info("Ignoring: " + e.getMessage())
        }

    def columns(expr: RelExpr): List[Column] =
        processor.handleQuery(expr.limit(0), {
            (ts: TableResult) => ts.columns
        })
}
