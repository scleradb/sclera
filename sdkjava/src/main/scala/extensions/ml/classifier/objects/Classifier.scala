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

package com.scleradb.java.analytics.ml.classifier.objects

import com.scleradb.sql.expr.ScalValueBase
import com.scleradb.sql.result.TableRow

import com.scleradb.analytics.ml.datatypes.DataAttribute
import com.scleradb.analytics.ml.classifier.objects.{Classifier => SClassifier}

/** Abstract base class for classifier libraries */
abstract class Classifier extends SClassifier {
    /** Classifier name */
    override val name: String
    /** Target attribute */
    override val targetAttr: DataAttribute

    /** Feature attributes */
    def featureAttrsArray: Array[DataAttribute]

    /** Feature attributes (Scala) */
    override lazy val featureAttrs: List[DataAttribute] =
        featureAttrsArray.toList

    /** Classify a row
      * @param t Table row to be classified;
      *        must contain the feature attribute columns
      * @return Assigned class label, if classification successful, else null
      */
    def classify(t: TableRow): ScalValueBase

    /** Classify a row (Scala)
      * @param t Table row to be classified;
      *        must contain the feature attribute columns
      * @return Assigned class label, if classification successful, else null
      */
    override def classifyOpt(t: TableRow): Option[ScalValueBase] =
        Option(classify(t))

    /** Classifier description */
    override def description: String
}
