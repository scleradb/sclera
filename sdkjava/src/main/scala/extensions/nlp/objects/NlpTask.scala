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

package com.scleradb.java.analytics.nlp.objects

import com.scleradb.java.sql.result.TableResult

import com.scleradb.sql.result.{TableResult => ScalaTableResult}
import com.scleradb.sql.expr.{ScalExpr, ColRef}

import com.scleradb.analytics.nlp.objects.{NlpTask => ScalaNlpTask}

/** Base class for NLP / Text mining task objects */
abstract class NlpTask extends ScalaNlpTask {
    /** Task name */
    override val name: String

    /** Input column containing the text */
    override val inputCol: ColRef

    /** Output columns */
    def resultColsArray: Array[ColRef]

    /** Output columns (Scala) */
    override lazy val resultCols: List[ColRef] = resultColsArray.toList

    /** Evaluate this task
      * @param rs Relational input - contains the metadata and row iterator
      * @return Output result
      */
    def evalTask(rs: TableResult): TableResult

    /** Evaluate this task (Scala)
      * @param rs Relational input - contains the metadata and row iterator
      * @return Output result
      */
    override def eval(rs: ScalaTableResult): ScalaTableResult =
        evalTask(TableResult(rs))
}
