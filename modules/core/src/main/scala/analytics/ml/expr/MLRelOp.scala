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

package com.scleradb.analytics.ml.expr

import com.scleradb.dbms.location.LocationId
import com.scleradb.sql.expr.{RelExpr, ExtendedRelOp, AnnotColRef, SortExpr}

import com.scleradb.analytics.ml.objects.MLObjectId

// machine learning extended relational operators
private[scleradb]
abstract class MLRelOp extends ExtendedRelOp {
    override def isStreamEvaluable(inputs: List[RelExpr]): Boolean = true

    override def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head.resultOrder

    override def tableNames(inputs: List[RelExpr]): List[String] = Nil

    override def starColumns(inputs: List[RelExpr]): List[AnnotColRef] =
        tableColRefs(inputs).map { col => AnnotColRef(None, col.name) }
}
