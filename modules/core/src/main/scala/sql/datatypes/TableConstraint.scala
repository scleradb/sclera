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

package com.scleradb.sql.datatypes

import com.scleradb.exec.Schema
import com.scleradb.dbms.location.LocationId

import com.scleradb.sql.expr.{ColRef, TableRefSource, TableRefTarget}
import com.scleradb.sql.expr.{TableRefSourceByName, TableRefSourceById}
import com.scleradb.sql.expr.{TableRefTargetByName, TableRefTargetById}
import com.scleradb.sql.objects.TableId

/** Abstract base class for all table constraints */
sealed abstract class TableConstraint

/** Primary key constraint
  * @param cols List of columns forming the primary key
  */
case class PrimaryKey(cols: List[ColRef]) extends TableConstraint

/** Foreign key reference constraint
  * @param cols List of columns forming the foreign key
  * @param refTableLocIdOpt Location id of the referenced table, if available
  * @param refTableName Name of the referenced table, if available
  * @param refCols Columns of the table referenced
  */
case class ForeignKey(
    cols: List[ColRef],
    refTableLocIdOpt: Option[LocationId],
    refTableName: String,
    refCols: List[ColRef] = Nil
) extends TableConstraint {
    def refTableId(schema: Schema): TableId =
        refTableLocIdOpt match {
            case Some(refTableLocId) => TableId(refTableLocId, refTableName)
            case None => TableRefSourceByName(schema, refTableName).tableId
        }

    def refTableSource(schema: Schema): TableRefSource =
        refTableLocIdOpt match {
            case Some(refTableLocId) =>
                TableRefSourceById(schema, TableId(refTableLocId, refTableName))
            case None => TableRefSourceByName(schema, refTableName)
        }

    def refTableTarget(schema: Schema): TableRefTarget =
        refTableLocIdOpt match {
            case Some(refTableLocId) =>
                TableRefTargetById(schema, TableId(refTableLocId, refTableName))
            case None => TableRefTargetByName(schema, refTableName)
        }
}
