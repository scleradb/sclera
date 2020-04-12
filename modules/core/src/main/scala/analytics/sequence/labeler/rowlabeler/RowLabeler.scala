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

package com.scleradb.analytics.sequence.labeler

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.expr.{ColRef, RelExpr}
import com.scleradb.sql.result.ScalTableRow

// abstract row labeler
abstract class RowLabeler {
    // columns required by this labeler
    def requiredCols: List[ColRef]
    def rowLabels(row: ScalTableRow): List[Label]
    def labeledExpr(input: RelExpr): RelExpr
    def isRedundant(labels: List[Label]): Boolean
    def repr: String
}
