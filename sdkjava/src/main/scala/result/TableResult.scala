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

package com.scleradb.java.sql.result

import scala.jdk.CollectionConverters._

import com.scleradb.sql.expr.SortExpr
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableRow, TableResult => ScalaTableResult}

/** Abstract base class for table results - Java version
  *
  * Encapsulates an intermediate result.
  * Contains the result metadata, and iterator on the result rows
  */
abstract class TableResult extends ScalaTableResult {
    /** List of columns in the result */
    def columnsArray: Array[Column]
    /** List of columns in the result (Scala) */
    override lazy val columns: List[Column] = columnsArray.toList

    /** Iterator over the rows in the result */
    def resultRows: java.util.Iterator[TableRow]
    /** Iterator over the rows in the result (Scala) */
    override def rows: Iterator[TableRow] = resultRows.asScala

    /** Ordering of the rows in the result */
    def sortExprs: Array[SortExpr]
    /** Ordering of the rows in the result (Scala) */
    override lazy val resultOrder: List[SortExpr] = Option(sortExprs) match {
        case Some(xs) => xs.toList
        case None => Nil
    }

    /** Close the result, and free the resources */
    override def close(): Unit
}

object TableResult {
    def apply(result: ScalaTableResult): TableResult = new TableResult {
        override def columnsArray: Array[Column] = result.columns.toArray

        override def resultRows: java.util.Iterator[TableRow] =
            result.rows.asJava

        override def sortExprs: Array[SortExpr] = result.resultOrder.toArray

        override def close(): Unit = result.close()
    }
}
