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

package com.scleradb.analytics.nlp.objects

import scala.util.matching.Regex.Match

import com.scleradb.sql.result.{TableResult, ExtendedTableRow}
import com.scleradb.sql.expr.{ColRef, SortExpr}
import com.scleradb.sql.expr.{ScalColValue, CharConst, SqlNull}
import com.scleradb.sql.types.SqlCharVarying
import com.scleradb.sql.datatypes.Column

import com.scleradb.analytics.nlp.service.NlpService

/** Class for parsing text using regex */
case class ParseTask(
    regexStr: String,
    override val inputCol: ColRef,
    override val resultCols: List[ColRef]
) extends NlpTask {
    override val name: String = "PARSE"

    override def eval(rs: TableResult): TableResult = new TableResult {
        override val columns: List[Column] =
            resultCols.map { col =>
                Column(col.name, SqlCharVarying(None).option)
            } ::: rs.columns

        override val resultOrder: List[SortExpr] = rs.resultOrder

        override def rows: Iterator[ExtendedTableRow] = rs.typedRows.map { t =>
            val text: String = t.getStringOpt(inputCol.name) getOrElse {
                throw new IllegalArgumentException(
                    "Column \"" + inputCol.repr + "\" not found"
                )
            }

            val resultMap: Map[String, ScalColValue] = Map() ++ {
                regexStr.r.findFirstMatchIn(text) match {
                    case Some(m) =>
                        resultCols.zipWithIndex.map { case (col, i) =>
                            val colVal: ScalColValue =
                                Option(m.group(i+1)) match {
                                    case Some(s) => CharConst(s)
                                    case None => SqlNull(SqlCharVarying(None))
                                }

                            col.name -> colVal
                        }
                    case None =>
                        resultCols.map { col =>
                            col.name -> SqlNull(SqlCharVarying(None))
                        }
                }
            }

            ExtendedTableRow(t, resultMap)
        }

        override def close(): Unit = { }
    }
}
