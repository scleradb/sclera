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

package com.scleradb.analytics.infertypes.datatypes

import com.scleradb.sql.expr.{ScalColValue, CharConst, ColRef, SqlNull}
import com.scleradb.sql.expr.SortExpr

import com.scleradb.sql.types.{SqlType, SqlChar}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.exec.{ScalTypeInferer, ScalCastEvaluator}
import com.scleradb.sql.result.{TableResult, TableRow, ScalTableRow}

import com.scleradb.analytics.infertypes.expr.InferTypes

private[scleradb]
class InferTypesTableResult(
    override val columns: List[Column],
    override val rows: Iterator[TableRow],
    override val resultOrder: List[SortExpr]
) extends TableResult {
    override def close(): Unit = { }
}

object InferTypesTableResult {
    def apply(
        cols: List[ColRef],
        nulls: List[String],
        lookAheadOpt: Option[Int],
        inputResult: TableResult
    ): InferTypesTableResult = {
        val it: Iterator[TableRow] = inputResult.rows
        val lookAhead: List[TableRow] = lookAheadOpt match {
            case Some(n) => it.take(n).toList
            case None => it.toList
        }

        val resultOrder: List[SortExpr] =
            inputResult.resultOrder takeWhile { se =>
                val seCols: Set[ColRef] = se.expr.colRefs
                !cols.exists(col => seCols contains col)
            }

        val infSqlTypes: Map[String, SqlType] = Map() ++
            cols.flatMap { col =>
                val vals: List[String] = lookAhead.flatMap { r =>
                    r.getStringOpt(col.name).map { s => s.trim }
                } filter { s => !nulls.contains(s) }

                ScalTypeInferer.inferType(vals) match {
                    case (_: SqlChar) => None // String inferred => ignore
                    case t => Some(col.name -> t)
                }
            }

        val columns: List[Column] = inputResult.columns.map { col =>
            infSqlTypes.get(col.name) match {
                case Some(t) => Column(col.name, t.option)
                case None => col
            }
        }

        val rows: Iterator[TableRow] = lookAhead.iterator ++ it
        val castRows: Iterator[TableRow] = if( infSqlTypes.isEmpty ) rows else {
            rows.map { row =>
                val valMap: Map[String, ScalColValue] = Map() ++
                    inputResult.columns.map { col =>
                        val v: ScalColValue = row.getScalExpr(col)
                        val castv: ScalColValue = 
                            (v, infSqlTypes.get(col.name)) match {
                                case (CharConst(s), Some(t))
                                if nulls.contains(s) =>
                                    SqlNull(t)

                                case (vexpr, Some(t)) =>
                                    ScalCastEvaluator.castScalColValue(vexpr, t)

                                case (vexpr, None) =>
                                    vexpr
                            }

                        col.name -> castv
                    }

                ScalTableRow(valMap)
            }
        }

        new InferTypesTableResult(columns, castRows, resultOrder)
    }
}
