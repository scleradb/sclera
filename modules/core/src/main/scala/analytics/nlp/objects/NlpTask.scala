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

package com.scleradb.analytics.nlp.objects

import com.scleradb.objects.DbObject

import com.scleradb.sql.result.TableResult
import com.scleradb.sql.expr.{RelExpr, SortExpr}
import com.scleradb.sql.expr.{ScalExpr, ColRef, CharConst}

import com.scleradb.analytics.nlp.service.NlpService

/** Base class for NLP / Text mining task objects */
abstract class NlpTask extends DbObject {
    /** Task name */
    override val name: String

    /** Input column containing the text */
    val inputCol: ColRef

    /** Output columns */
    val resultCols: List[ColRef]

    /** Names of the columns in the result
      *
      * @param inputs Input expressions
      * @return Columns in the result
      */
    def tableColRefs(inputs: List[RelExpr]): List[ColRef] =
        resultCols:::inputs.head.tableColRefs

    /** The sort order of the result
      *
      * @param inputs Input expressions
      * @return Sort order of the result
      */
    def resultOrder(inputs: List[RelExpr]): List[SortExpr] =
        inputs.head.resultOrder

    /** Evaluate this object
      * @param rs Relational input - contains the metadata and row iterator
      * @return Output result
      */
    def eval(rs: TableResult): TableResult
}

object NlpTask {
    def apply(
        libOpt: Option[String],
        langOpt: Option[String],
        name: String,
        args: List[ScalExpr],
        inputCol: ColRef,
        resColsSpec: List[ColRef]
    ): NlpTask = (libOpt, name, args) match {
        case (None, "PARSE", List(CharConst(regexStr))) =>
            ParseTask(regexStr, inputCol, resColsSpec)
        case _ =>
            NlpService(libOpt).createObject(
                langOpt, name, args, inputCol, resColsSpec
            )
    }
}
