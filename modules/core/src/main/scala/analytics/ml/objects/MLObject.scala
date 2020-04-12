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

package com.scleradb.analytics.ml.objects

import java.io.{ObjectInputStream, ObjectOutputStream}

import org.slf4j.{Logger, LoggerFactory}

import com.scleradb.exec.Schema

import com.scleradb.dbms.location.LocationId
import com.scleradb.objects.{DbObject, SchemaObject, SchemaObjectId}
import com.scleradb.analytics.ml.service.MLService

/** Abstract base class for analytics engine libraries */
abstract class MLObject extends DbObject {
    /** Logger */
    val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

    /** Service identifier */
    def serviceId: String

    /** Type of object */
    def typeStr: String

    /** Name of this object */
    override val name: String

    /** Description of this object */
    def description: String

    /** Associated Schema Object */
    def schemaObject: SchemaMLObject

    /** Serialize this object */
    def serialize(out: ObjectOutputStream): Unit = {
        out.writeUTF(serviceId)
        out.writeUTF(typeStr)
    }
}

/** Identifier for ML Objects */
case class MLObjectId(override val name: String) extends SchemaObjectId {
    override def repr: String = name.toUpperCase
}

abstract class SchemaMLObject extends SchemaObject {
    override val obj: MLObject
    override val id: MLObjectId = MLObjectId(obj.name)

    /** Type of object */
    override def typeStr: String = obj.typeStr

    /** Location of the object
      * None, because the object is not associated with a location
      */
    override def locationIdOpt: Option[LocationId] = None
}

object SchemaMLObject {
    def serialize(sv: SchemaMLObject, out: ObjectOutputStream): Unit =
        sv.obj.serialize(out)

    def deSerialize(in: ObjectInputStream): SchemaMLObject = {
        val serviceId: String = in.readUTF()
        val typeStr: String = in.readUTF()

        apply(MLService(serviceId).deSerialize(in, typeStr))
    }

    def apply(obj: MLObject): SchemaMLObject = obj.schemaObject

    def objectsByName(schema: Schema, name: String): List[SchemaMLObject] =
        objectOpt(schema, MLObjectId(name)).toList

    def objectOpt(schema: Schema, id: MLObjectId): Option[SchemaMLObject] =
        objectOpt(schema, id.repr)

    def objectOpt(schema: Schema, id: String): Option[SchemaMLObject] =
        schema.mlObjectOpt(id)

    def objects(schema: Schema): List[SchemaMLObject] = schema.mlObjects
}
