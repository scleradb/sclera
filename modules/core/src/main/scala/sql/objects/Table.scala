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

import scala.language.postfixOps

import com.scleradb.exec.Schema
import com.scleradb.dbms.location.{Location, LocationId}

import com.scleradb.sql.expr.{ColRef, TableRefTarget}
import com.scleradb.sql.datatypes.{Column, PrimaryKey, ForeignKey}
import com.scleradb.sql.types.SqlOption

/** Relational table
  *
  * @param name Name of the table
  * @param columns Columns of the table
  * @param keyOpt Primary key of the table (optional)
  * @param foreignKeys Foreign keys of the table (optional)
  * @param baseType Type of this table structure (base table or view)
  */
class Table(
    override val name: String,
    val columns: List[Column],
    val keyOpt: Option[PrimaryKey],
    val foreignKeys: List[ForeignKey],
    val baseType: Table.BaseType
) extends Relation {
    /** Map of column name to column objects */
    lazy val columnMap: Map[String, Column] =
        Map() ++ columns.map(col => (col.name.toUpperCase -> col))

    /** Returns the column object for the given column name, if present
      *
      * @param name Column name
      * @return The associated column object, if present in the table's schema
      */
    def column(name: String): Option[Column] = columnMap.get(name.toUpperCase)

    /** Returns the column object for the given column ref, if present
      *
      * @param name Column ref
      * @return The associated column object, if present in the table's schema
      */
    def column(colRef: ColRef): Option[Column] = column(colRef.name)

    /** List of primary key columns */
    lazy val keyCols: List[Column] = keyOpt match {
        case Some(key) => key.cols.map { col => column(col).get }
        case None => Nil
    }

    /** List of tables referenced by foreign keys in this table */
    def targets(schema: Schema): List[TableId] =
        foreignKeys.map { fk => fk.refTableId(schema) } distinct

    override val columnRefs: List[ColRef] =
        columns.map { col => ColRef(col.name) }
}

/** Companion object - contains helper functions, helper objects
  * and alternative constructor that performs constraint validations before
  * creating a Table object.
  */
object Table {
    /** Type of the underlying table object */
    sealed abstract class BaseType {
        /** String representation of the object */
        val repr: String
    }

    /** Base table */
    case object BaseTable extends BaseType {
        override val repr: String = "TABLE"
    }

    /** Virtual view */
    case object BaseView extends BaseType {
        override val repr: String = "VIEW"
    }

    /** Companion object - contains helper constructor */
    object BaseType {
        private val map: Map[String, BaseType] =
            Map(BaseTable.repr -> BaseTable, BaseView.repr -> BaseView)

        /** Creates a BaseType object corresponding to the input string
          *
          * @param typeStr String containing the base type
          */
        def apply(typeStr: String): BaseType =
            map.get(typeStr.toUpperCase) getOrElse {
                throw new IllegalArgumentException(
                    "Invalid base table type: \"" + typeStr + "\""
                )
            }
    }

    /** Converts the primary key columns to NOT NULL
      * before creating the Table object
      *
      * @param name Name of the table
      * @param columns Columns of the table
      * @param keyOpt Primary key of the table (optional)
      * @param foreignKeys Foreign keys of the table (optional)
      * @param baseType Type of this table structure (base table or view)
      */
    def apply(
        name: String,
        columns: List[Column],
        keyOpt: Option[PrimaryKey],
        foreignKeys: List[ForeignKey],
        baseType: BaseType
    ): Table = {
        val pkColNames: List[String] = keyOpt match {
            case Some(pk) => pk.cols.map { col => col.name }
            case None => Nil
        }

        val updatedColumns: List[Column] = columns.map {
            case Column(name, SqlOption(sqlType), familyOpt)
            if( pkColNames contains name ) =>
                Column(name, sqlType, familyOpt)
            case col => col
        }

        new Table(name, updatedColumns, keyOpt, foreignKeys, baseType)
    }
}

case class TableId(
    locationId: LocationId,
    override val name: String
) extends RelationId {
    override def repr: String = locationId.repr + "." + name
}

class SchemaTable(
    override val obj: Table,
    val locationId: LocationId
) extends SchemaRelation {
    override val id: TableId = TableId(locationId, obj.name)

    override def typeStr: String = "TABLE [BASE = " + obj.baseType.repr + "]"

    override def locationIdOpt: Option[LocationId] = Some(locationId)
}

object SchemaTable {
    def apply(table: Table, locationId: LocationId): SchemaTable =
        new SchemaTable(table, locationId)

    def objects(
        schema: Schema,
        locIdOpt: Option[LocationId],
        nameOpt: Option[String]
    ): List[SchemaTable] = (locIdOpt, nameOpt) match {
        case (Some(locId), Some(name)) =>
            objectOpt(schema, TableId(locId, name)).toList
        case (None, Some(name)) =>
            objectsByName(schema, name)
        case (Some(locId), None) =>
            objectsByLocation(schema, List(locId))
        case (None, None) =>
            objects(schema)
    }
    
    def objects(schema: Schema): List[SchemaTable] = schema.tables

    def objectsByName(schema: Schema, name: String): List[SchemaTable] =
        Location.locationIds(schema).flatMap { locId =>
            objectOpt(schema, TableId(locId, name))
        }

    def objectsByLocation(
        schema: Schema,
        locIds: List[LocationId]
    ): List[SchemaTable] = schema.tables(locIds)

    def objectOpt(schema: Schema, id: TableId): Option[SchemaTable] =
        objectOpt(schema, id.repr)

    def objectOpt(schema: Schema, id: String): Option[SchemaTable] =
        schema.tableOpt(id)
}
