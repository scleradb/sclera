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

package com.scleradb.java.external.objects

import com.scleradb.sql.expr.SortExpr
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableResult

import com.scleradb.external.objects.{ExternalSource => ScalaExternalSource}

/** Abstract base class for external data sources - Java version */
abstract class ExternalSource extends ScalaExternalSource {
    /** Name of the data source */
    override val name: String

    /** Columns of the rows emitted by the data source */
    def columnsArray: Array[Column]
    /** Columns of the rows emitted by the data source (Scala) */
    override lazy val columns: List[Column] =
        Option(columnsArray) match {
            case Some(cs) => cs.toList
            case None => // columnsList is null
                throw new RuntimeException(
                    "Datasource implementation error: columnList is null"
                )
        }

    /** Sort order of the rows emitted by the data source */
    def sortExprs: Array[SortExpr]

    /** The rows emitted by the data source */
    override def result: TableResult
}
