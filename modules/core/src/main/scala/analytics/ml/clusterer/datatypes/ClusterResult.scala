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

package com.scleradb.analytics.ml.clusterer.datatypes

import com.scleradb.sql.types.SqlInteger
import com.scleradb.sql.expr.{ScalColValue, IntConst, SqlNull, ColRef, SortExpr}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.{TableResult, ExtendedTableRow}

import com.scleradb.analytics.ml.clusterer.objects.Clusterer

private[scleradb]
class ClusterResult(
    rs: TableResult,
    clusterIdColRef: ColRef,
    clusterer: Clusterer
) extends TableResult {
    if( rs.columns.map(col => col.name).contains(clusterIdColRef.name) )
        throw new IllegalArgumentException(
            "Column \"" + clusterIdColRef.name + "\" is already present " +
            "in the input"
        )

    override val columns: List[Column] =
        Column(clusterIdColRef.name, SqlInteger)::rs.columns

    override val rows: Iterator[ExtendedTableRow] = rs.typedRows.map { t =>
        val clusterId: Int = clusterer.cluster(t)
        ExtendedTableRow( t, Map(clusterIdColRef.name -> IntConst(clusterId)))
    }

    override val resultOrder: List[SortExpr] = rs.resultOrder

    override def close(): Unit = { }
}
