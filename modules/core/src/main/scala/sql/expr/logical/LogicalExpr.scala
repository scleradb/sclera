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

package com.scleradb.sql.expr

import com.scleradb.exec.Schema

/** Base of all logical expressions */
abstract class LogicalExpr extends Serializable {
    /** Associated schema */
    val schema: Schema

    /** View this expression as a relational expression */
    val tableView: RelExpr

    /** columns of this relational expression */
    val tableColRefs: List[ColRef]
}
