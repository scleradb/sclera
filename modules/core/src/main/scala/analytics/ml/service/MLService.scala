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

package com.scleradb.analytics.ml.service

import java.io.ObjectInputStream

import com.scleradb.service.{ScleraService, ScleraServiceLoader}

import com.scleradb.sql.result.TableResult
import com.scleradb.sql.expr.ColRef

import com.scleradb.analytics.ml.objects.MLObject
import com.scleradb.analytics.ml.classifier.objects.Classifier
import com.scleradb.analytics.ml.clusterer.objects.Clusterer

import com.scleradb.config.ScleraConfig

/** Machine learning operator service */
abstract class MLService extends ScleraService {
    def deSerialize(in: ObjectInputStream, typeStr: String): MLObject =
        if( typeStr == Classifier.typeStr )
            deSerializeClassifier(in)
        else if( typeStr == Clusterer.typeStr )
            deSerializeClusterer(in)
        else throw new IllegalArgumentException(
            "Objects of type \"" + typeStr + "\" are not supported"
        )

    /** Deserialize classifier
      * @param in Input stream containing the seralized classifier
      * @return The deserialized classifier
      */
    def deSerializeClassifier(in: ObjectInputStream): Classifier
    /** Deserialize clusterer
      * @param in Input stream containing the seralized clusterer
      * @return The deserialized clusterer
      */
    def deSerializeClusterer(in: ObjectInputStream): Clusterer

    /** Create classifier
      * @param name Name of the classifier
      * @param specOpt Classifier specification
      * @param targetColIndex Index of the target column in the training data
      * @param numDistinctValuesMap Distinct values for each column, if known
      * @param tableResult Training data
      * @return Created classifier
      */
    def createClassifier(
        name: String,
        specOpt: Option[(String, String)],
        targetColIndex: Int,
        numDistinctValuesMap: Map[ColRef, Int],
        tableResult: TableResult
    ): Classifier

    /** Create clusterer
      * @param name Name of the clusterer
      * @param specOpt Clusterer specification
      * @param numDistinctValuesMap Distinct values for each column, if known
      * @param tableResult Training data
      * @return Created clusterer
      */
    def createClusterer(
        name: String,
        specOpt: Option[(String, String)],
        numDistinctValuesMap: Map[ColRef, Int],
        tableResult: TableResult
    ): Clusterer
}

object MLService extends ScleraServiceLoader(classOf[MLService]) {
    def apply(idOpt: Option[String] = None): MLService =
        apply(idOpt getOrElse ScleraConfig.defaultMLService)
}
