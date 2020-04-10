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

import com.scleradb.sql.result.TableResult

import com.scleradb.external.objects.{ExternalTarget => ScalaExternalTarget}

/** Abstract base class for external data targets - Java version */
abstract class ExternalTarget extends ScalaExternalTarget {
    /** Name of the data target */
    override val name: String

    /** Write the data in the table result */
    override def write(ts: TableResult): Unit
}
