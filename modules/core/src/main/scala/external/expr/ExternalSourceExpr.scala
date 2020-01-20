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

package com.scleradb.external.expr

import com.scleradb.exec.Schema
import com.scleradb.sql.expr.{ColRef, RelBaseExpr, SortExpr}
import com.scleradb.dbms.location.LocationId
import com.scleradb.external.objects.ExternalSource

// external source
private [scleradb]
case class ExternalSourceExpr(
    override val schema: Schema,
    source: ExternalSource
) extends RelBaseExpr {
    override val name: String = source.name

    override val locationIdOpt: Option[LocationId] = None
    override val isStreamEvaluable: Boolean = true
    override val isEvaluable: Boolean = true

    override val tableColRefs: List[ColRef] =
        source.columns.map { col => ColRef(col.name) }

    override val resultOrder: List[SortExpr] = source.result.resultOrder
}
