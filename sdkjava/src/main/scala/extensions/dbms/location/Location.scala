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

package com.scleradb.java.dbms.location

import scala.jdk.CollectionConverters._

import com.scleradb.exec.Schema

import com.scleradb.dbms.driver.StatementDriver
import com.scleradb.dbms.location.{LocationId, LocationPermit}
import com.scleradb.dbms.location.{Location => ScalaLocation}

/**  Abstract base class for all locations */
abstract class Location extends ScalaLocation {
    /** Associated schema */
    override val schema: Schema

    /** Location identifier */
    override val id: LocationId

    /** Is this a temporary location? */
    override val isTemporary: Boolean

    /** Underlying datastore name */
    override val dbms: String

    /** Underlying database schema, if any */
    val dbSchema: String
    override val dbSchemaOpt: Option[String] = Option(dbSchema)

    /** Read-write permit */
    override val permit: LocationPermit

    /** Datastore connection parameter */
    override val param: String

    /** Configuration parameters - key/value pairs */
    def configMap: java.util.Map[java.lang.String, java.lang.String]

    /** Configuration parameters - key/value pairs (Scala) */
    override lazy val config: List[(String, String)] =
        configMap.entrySet.asScala.toList.map { e =>
            (e.getKey(), e.getValue())
        }

    /** Driver for this location */
    override def driver: StatementDriver
}
