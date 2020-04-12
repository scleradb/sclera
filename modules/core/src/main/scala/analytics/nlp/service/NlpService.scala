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

package com.scleradb.analytics.nlp.service

import com.scleradb.service.{ScleraService, ScleraServiceLoader}
import com.scleradb.config.ScleraConfig

import com.scleradb.sql.expr.{ScalExpr, ColRef}

import com.scleradb.analytics.nlp.objects.NlpTask

/** NLP operator service */
abstract class NlpService extends ScleraService {
    /** Create an NLP task object
      * @param langOpt Language code, optional
      * @param taskName Task name (follows `TEXT` in the SQL query)
      * @param args Task arguments
      * @param inputCol Input column containing the text
      * @param resultCols Output columns containg the task results
      * @return The constructed task object
      */
    def createObject(
        langOpt: Option[String],
        taskName: String,
        args: List[ScalExpr],
        inputCol: ColRef,
        resultCols: List[ColRef]
    ): NlpTask
}

object NlpService extends ScleraServiceLoader(classOf[NlpService]) {
    def apply(idOpt: Option[String] = None): NlpService =
        apply(idOpt getOrElse ScleraConfig.defaultNlpService)
}
