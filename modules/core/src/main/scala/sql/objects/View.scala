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

package com.scleradb.sql.objects

import java.io.{ObjectInputStream, ObjectOutputStream, NotSerializableException}

import com.scleradb.exec.Schema

import com.scleradb.dbms.location.LocationId

import com.scleradb.sql.expr.{ColRef, RelExpr}

/** Relational virtual view
  *
  * @param name Name of the view
  * @param expr Relational expression (query) associated with the virtual view
  */
class View(
    override val name: String,
    val expr: RelExpr
) extends Relation {
    override val columnRefs: List[ColRef] = expr.tableColRefs
}

/** Companion object - contains the constructor */
object View {
    def apply(name: String, expr: RelExpr): View = new View(name, expr)
}

private[scleradb]
case class ViewId(override val name: String) extends RelationId {
    override def repr: String = name.toUpperCase
}

private[scleradb]
class SchemaView(override val obj: View) extends SchemaRelation {
    override val id: ViewId = ViewId(obj.name)

    override def typeStr: String = "VIEW"

    override def locationIdOpt: Option[LocationId] = None
}

private[scleradb]
object SchemaView {
    def apply(view: View): SchemaView = new SchemaView(view)

    def serialize(sv: SchemaView, out: ObjectOutputStream): Unit =
        try {
            out.writeUTF(sv.obj.name)
            RelExpr.serialize(sv.obj.expr, out)
        } catch { case (e: NotSerializableException) =>
            throw new IllegalArgumentException(
                "Serialization failure: " + e.getMessage(), e
            )
        }

    def deSerialize(schema: Schema, in: ObjectInputStream): SchemaView = {
        val name: String = in.readUTF()
        val expr: RelExpr = RelExpr.deSerialize(schema, in)
        SchemaView(View(name, expr))
    }

    def objectsByName(schema: Schema, name: String): List[SchemaView] =
        objectOpt(schema, ViewId(name)).toList

    def objectOpt(schema: Schema, id: ViewId): Option[SchemaView] =
        objectOpt(schema, id.repr)

    def objectOpt(schema: Schema, id: String): Option[SchemaView] =
        schema.viewOpt(id)

    def objects(schema: Schema): List[SchemaView] = schema.views
}
