/**
* Sclera Extensions - Java SDK
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

package com.scleradb.java.dbms.service

import com.scleradb.exec.Schema

import com.scleradb.dbms.location.{LocationId, Location, LocationPermit}
import com.scleradb.dbms.service.{DBService => ScalaDBService}

abstract class DBService extends ScalaDBService {
    /** Create a new data source location
      * @param id Identifier of the new location
      * @param param Data source identifier (e.g. db name for RDBMS)
      * @param dbSchema Underlying database schema, null if unspecified
      * @param config Connection configuration parameters
      * @param permit Indicates a read-only vs read-write location
      */
    def createLocation(
        id: LocationId,
        param: String,
        dbSchema: String,
        config: java.util.Map[java.lang.String, java.lang.String],
        permit: LocationPermit
    ): Location

    /** Create a new data source location (Scala)
      * @param schema Associated schema
      * @param id Identifier of the new location
      * @param param Data source identifier (e.g. db name for RDBMS)
      * @param config Connection configuration parameters
      * @param permit Indicates a read-only vs read-write location
      */
    override def createLocation(
        schema: Schema,
        id: LocationId,
        param: String,
        dbSchemaOpt: Option[String],
        config: List[(String, String)],
        permit: LocationPermit
    ): Location = {
        val configMap: java.util.Map[String, String] = new java.util.HashMap()
        config.foreach { case (k, v) => configMap.put(k, v) }

        val dbSchema: String = dbSchemaOpt getOrElse null.asInstanceOf[String]
        createLocation(id, param, dbSchema, configMap, permit)
    }
}
