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

package com.scleradb.analytics.ml.imputer.datatypes

import com.scleradb.exec.Schema

import com.scleradb.sql.expr._
import com.scleradb.sql.types.SqlBool
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ExtendedTableRow}

import com.scleradb.analytics.ml.classifier.objects.Classifier

class ImputeResult(
    schema: Schema,
    rs: TableResult,
    imputeSpecs: List[ImputeSpec]
) extends TableResult {
    private val rsColNames: List[String] = rs.columns.map { col => col.name }
    imputeSpecs.foreach { spec =>
        spec.flagColRefOpt.foreach { colRef =>
            if( rsColNames contains colRef.name )
                throw new IllegalArgumentException(
                    "Column \"" + colRef.name +
                    "\" is already present in the input"
                )
        }
    }
    
    override val columns: List[Column] = {
        val specMap: Map[String, ImputeSpec] = Map() ++
            imputeSpecs.map { spec => (spec.imputeColRef.name -> spec) }

        rs.columns.flatMap { col =>
            specMap.get(col.name) match {
                case Some(spec) => spec.flagColRefOpt match {
                    case Some(fColRef) =>
                        List(col, Column(fColRef.name, SqlBool))
                    case None => List(col)
                }

                case None => List(col)
            }
        }
    }

    override def rows: Iterator[ExtendedTableRow] = rs.typedRows.map { t =>
        val imputeValMap: Map[String, ScalColValue] = Map() ++
            imputeSpecs.flatMap { spec =>
                val classifier: Classifier = spec.classifier(schema)

                t.getScalExpr(spec.imputeColRef.name) match {
                    case (sqlNull: SqlNull) =>
                        // target value missing, needs imputation
                        val targetVal: ScalColValue =
                            classifier.classifyOpt(t) getOrElse sqlNull

                        (spec.imputeColRef.name -> targetVal)::
                        spec.flagColRefOpt.toList.map { flagColRef =>
                            (flagColRef.name -> BoolConst(true))
                        }

                    case (_: ScalValueBase) => // imputation not needed
                        spec.flagColRefOpt.toList.map { flagColRef =>
                            (flagColRef.name -> BoolConst(false))
                        }
                }
            }

        ExtendedTableRow(t, imputeValMap)
    }

    override val resultOrder: List[SortExpr] = rs.resultOrder

    override def close(): Unit = { }
}
