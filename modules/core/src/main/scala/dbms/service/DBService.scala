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

package com.scleradb.dbms.service

import com.scleradb.exec.Schema

import com.scleradb.service.{ScleraService, ScleraServiceLoader}
import com.scleradb.dbms.location.{LocationId, Location, LocationPermit}

abstract class DBService extends ScleraService {
    /** Create a new data source location
      * @param schema Associated schema
      * @param id Identifier of the new location
      * @param param Data source identifier (e.g. db name for RDBMS)
      * @param dbSchemaOpt Underlying database schema, if any
      * @param config Connection configuration parameters
      * @param permit Indicates a read-only vs read-wriet location
      */
    def createLocation(
        schema: Schema,
        id: LocationId,
        param: String,
        dbSchemaOpt: Option[String],
        config: List[(String, String)],
        permit: LocationPermit
    ): Location
}

object DBService extends ScleraServiceLoader(classOf[DBService])
