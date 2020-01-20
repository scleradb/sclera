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

package com.scleradb.objects

import com.scleradb.exec.Schema
import com.scleradb.dbms.location.LocationId

/** Abstract class for all database objects */
abstract class DbObject extends Serializable {
    /** Name of this object */
    val name: String
}

// Database object global id
private[scleradb]
abstract class SchemaObjectId extends Serializable {
    val name: String
    def repr: String
}

// global class wrapping a local object
private[scleradb]
abstract class SchemaObject extends Serializable {
    val obj: DbObject
    val id: SchemaObjectId

    /** Name of this object */
    def name: String = obj.name

    /** Type of object */
    def typeStr: String

    /** Location of this object, if any */
    def locationIdOpt: Option[LocationId]

    override def toString: String = "[Object (id = " + id.repr + ")]"
}

private[scleradb]
object SchemaObject {
    def duration(schema: Schema, id: SchemaObjectId): DbObjectDuration =
        duration(schema, id.repr)

    def duration(schema: Schema, id: String): DbObjectDuration =
        schema.duration(id)

    def objectOpt(schema: Schema, id: SchemaObjectId): Option[SchemaObject] =
        objectOpt(schema, id.repr)

    def objectOpt(schema: Schema, id: String): Option[SchemaObject] =
        schema.objectOpt(id)

    def objects(schema: Schema): List[SchemaObject] = schema.objects
}
