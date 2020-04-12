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

package com.scleradb.analytics.ml.classifier.datatypes

import com.scleradb.sql.expr.{ColRef, ScalColValue, SqlNull, SortExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ExtendedTableRow}

import com.scleradb.analytics.ml.classifier.objects.Classifier

class ClassifyResult(
    rs: TableResult,
    targetColRef: ColRef,
    classifier: Classifier
) extends TableResult {
    if( rs.columns.map(col => col.name).contains(targetColRef.name) )
        throw new IllegalArgumentException(
            "Column \"" + targetColRef.name + "\" is already present " +
            "in the input"
        )

    override val columns: List[Column] =
        Column(targetColRef.name, classifier.targetAttr.sqlType.option)::
        rs.columns

    override def rows: Iterator[ExtendedTableRow] = rs.typedRows.map { t =>
        val targetVal: ScalColValue = classifier.classifyOpt(t) getOrElse {
            SqlNull(classifier.targetAttr.sqlType)
        }

        ExtendedTableRow(t, Map(targetColRef.name -> targetVal))
    }

    override val resultOrder: List[SortExpr] = rs.resultOrder

    override def close(): Unit = { }
}
