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

package com.scleradb.sql.result

import java.sql.{Time, Timestamp, Date}

import com.scleradb.util.tools.Format

import com.scleradb.sql.expr.{ScalExpr, ScalColValue, ColRef, SortExpr}
import com.scleradb.sql.datatypes.Column

/** Abstract base class for table results
  *
  * Encapsulates an intermediate result.
  * Contains the result metadata, and iterator on the result rows
  */
abstract class TableResult {
    /** List of columns in the result */
    val columns: List[Column]
    /** Iterator over the rows in the result */
    def rows: Iterator[TableRow]
    /** Ordering of the rows in the result */
    val resultOrder: List[SortExpr]

    /** Map from column name to column object */
    lazy val columnMap: Map[String, Column] =
        Map() ++ columns.map(col => (col.name.toUpperCase -> col))

    /** Returns the column object associated with the given name, if present
      * @param name Name of the column (case-insensitive)
      */
    def columnOpt(name: String): Option[Column] =
        columnMap.get(name.toUpperCase)

    /** Returns the column object associated with the given name
      * @param name Name of the column (case-insensitive)
      */
    def column(name: String): Column =
        columnOpt(name) getOrElse {
            throw new IllegalArgumentException(
                "Column \"" + name + "\" not found"
            )
        }

    /** Returns the column object associated with the given name, if present
      * @param colRef Reference of the column (case-insensitive)
      */
    def columnOpt(col: ColRef): Option[Column] = columnOpt(col.name)

    /** Returns the column object associated with the given name
      * @param colRef Reference of the column (case-insensitive)
      */
    def column(col: ColRef): Column = column(col.name)

    /** Iterator over the rows in the result with included schema information */
    def typedRows: Iterator[ScalTableRow] = rows.map {
        case (t: ScalTableRow) => t
        case t => new ScalTableRow {
            override def getScalExprOpt(cname: String): Option[ScalColValue] =
                columnOpt(cname).map { col => t.getScalExpr(col) }
        }
    }

    /** Format the table result (assumed finite) */
    def format: List[String] = {
        val cnames: List[String] = columns.map { col => col.name }
        val outRows: Iterator[List[String]] = rows.map { t =>
            cnames.map { cname => t.getStringOpt(cname).getOrElse("NULL") }
        }

        Format.formatResultSet(cnames, outRows.toList)
    }

    /** Close the result, and free the resources */
    def close(): Unit
}

/** Result containing rows constructed using column name to value maps
  *
  * @param columns List of columns in the result
  * @param vals Iterator on column name to value maps
  * @param resultOrder Ordering of the rows in the result
  */
class ScalTableResult(
    override val columns: List[Column],
    vals: Iterator[Map[String, ScalColValue]],
    override val resultOrder: List[SortExpr]
) extends TableResult {
    override def rows: Iterator[ScalTableRow] =
        vals.map { v => ScalTableRow(v) }
    override def close(): Unit = { /* ignore */ }
}

/** Companion object containing the constructor for ScalTableResult */
object ScalTableResult {
    /** Constructor for ScalTableResult
      *
      * @param columns List of columns in the result
      * @param vals Iterator on column name to value maps
      * @param resultOrder Ordering of the rows in the result
      */
    def apply(
        columns: List[Column],
        vals: Iterator[Map[String, ScalColValue]] = Iterator(),
        resultOrder: List[SortExpr] = Nil
    ): ScalTableResult = new ScalTableResult(columns, vals, resultOrder)
}
