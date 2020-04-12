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

package com.scleradb.dbms.location

import org.slf4j.{Logger, LoggerFactory}

import com.scleradb.config.ScleraConfig

import com.scleradb.exec.Schema

import com.scleradb.dbms.driver.StatementDriver
import com.scleradb.dbms.service.DBService

import com.scleradb.sql.expr.RelExpr

/**  Abstract base class for all locations */
abstract class Location {
    /** Associated schema */
    val schema: Schema

    /** Location identifier */
    val id: LocationId

    /** Is this a temporary location? */
    val isTemporary: Boolean
    /** Underlying datastore name */
    val dbms: String
    /** Underlying database schema, if any */
    val dbSchemaOpt: Option[String]
    /** Read-write permit */
    val permit: LocationPermit
    /** Is this a writeable location? */
    def isWritable: Boolean = permit.isWritable

    /** Datastore connection parameter */
    val param: String
    /** Configuration parameters - key/value pairs */
    val config: List[(String, String)]
    /** Driver for this location */
    def driver: StatementDriver

    /** Supported function list - optional */
    def supportedFunctionsOpt: Option[List[String]] = None

    /** Table name annotated with db schema, if specified */
    def annotTableName(name: String): String =
        dbSchemaOpt.map { s => s + "." + name } getOrElse name
}

object Location {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

    val schemaLocationId: LocationId = LocationId("SCHEMA")

    val reservedLocIds: List[LocationId] = List(schemaLocationId)
    def isReserved(locId: LocationId): Boolean =
        Location.reservedLocIds.map { id => id.repr } contains locId.repr

    def isSystemExpr(relExpr: RelExpr): Boolean =
        relExpr.locationIdOpt.exists { id => Location.isReserved(id) }

    val tempLocationId: LocationId = LocationId("TEMPDB")

    // data cache location
    // assigned to the tempLocationId unless specified otherwise
    private var dataCacheLocationIdVar: LocationId =
        ScleraConfig.dataCacheLocationIdOpt match {
            case Some(locStr) => LocationId(locStr)
            case None => tempLocationId
        }

    def dataCacheLocationId: LocationId = dataCacheLocationIdVar
    def dataCacheLocation(schema: Schema): Location =
        dataCacheLocationId.location(schema)

    // default location
    private var defaultLocationIdOpt: Option[LocationId] =
        ScleraConfig.defaultLocationIdOpt.map { locStr => LocationId(locStr) }
    def defaultLocationId: LocationId = defaultLocationIdOpt getOrElse {
        throw new IllegalArgumentException(
            "Cannot determine the location. " +
            "Please specify an explicit location, or " +
            "set the default location using \"set default location = ...\" " +
            "or by adding the property \"sclera.location.default\" " +
            "in the configuration."
        )
    }

    def configLocation(param: String, locId: LocationId): Unit =
        param.toUpperCase match {
            case "DEFAULT" => defaultLocationIdOpt = Some(locId)
            case "CACHE" => dataCacheLocationIdVar = locId
        }

    def isDefault(locId: LocationId): Boolean = defaultLocationIdOpt match {
        case None => true
        case Some(defLocId) => locId == defLocId
    }

    def isCache(locId: LocationId): Boolean = locId == dataCacheLocationId

    def locationIds(schema: Schema): List[LocationId] = schema.locationIds
    def locations(schema: Schema): List[Location] = schema.locations
    def locationOpt(schema: Schema, id: String): Option[Location] =
        schema.locationOpt(id)
    def locationOpt(schema: Schema, id: LocationId): Option[Location] =
        schema.locationOpt(id)

    def allLocations(schema: Schema): List[Location] = schema.allLocations
    def isActive(schema: Schema, id: LocationId): Boolean =
        schema.isActiveLocation(id)

    def apply(
        schema: Schema,
        id: LocationId,
        dbms: String,
        param: String,
        dbSchemaOpt: Option[String],
        config: List[(String, String)] = Nil,
        permit: LocationPermit = ReadWriteLocation
    ): Location =
        try DBService(dbms).createLocation(
            schema, id, param, dbSchemaOpt, config, permit
        ) catch { case (e: Throwable) =>
            logger.error(
                "Could not connect to database " + param +
                " from location " + id.repr + " (" + dbms + "): " +
                e.getMessage()
            )

            throw e
        }

    def apply(
        schema: Schema,
        id: LocationId,
        dbms: String,
        params: List[String],
        dbSchemaOpt: Option[String],
        permit: LocationPermit
    ): Location = {
        val param: String = params.headOption getOrElse {
            throw new IllegalArgumentException(
                "Location \"" + id + "\": Parameters not specified"
            )
        }

        // parse the key/value pairs
        val config: List[(String, String)] = params.tail.map { str =>
            str.split("=") match {
                case Array(k, v) => (k.trim, v.trim)
                case _ =>
                    throw new IllegalArgumentException(
                        "Location \"" + id + "\": " +
                        "Invalid parameter: \"" + str + "\""
                    )
            }
        }

        apply(schema, id, dbms, param, dbSchemaOpt, config, permit)
    }
}

/** Location identifier
  *
  * @param name Location name
  */
case class LocationId(name: String) {
    /** String representation of this location */
    val repr: String = name.toUpperCase

    /** Location object identified by this identifier */
    def location(schema: Schema): Location =
        Location.locationOpt(schema, this) getOrElse {
            throw new IllegalArgumentException(
                "Uninitialized location: " + repr
            )
        }
}
