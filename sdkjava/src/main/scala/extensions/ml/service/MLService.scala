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

package com.scleradb.java.analytics.ml.service

import java.io.ObjectInputStream

import com.scleradb.java.sql.result.TableResult

import com.scleradb.sql.result.{TableResult => ScalaTableResult}
import com.scleradb.sql.expr.ColRef

import com.scleradb.analytics.ml.classifier.objects.Classifier
import com.scleradb.analytics.ml.clusterer.objects.Clusterer

import com.scleradb.analytics.ml.service.{MLService => ScalaMLService}

/** Machine learning operator service */
abstract class MLService extends ScalaMLService {
    /** Deserialize classifier
      * @param in Input stream containing the seralized classifier
      * @return The deserialized classifier
      */
    override def deSerializeClassifier(in: ObjectInputStream): Classifier
    /** Deserialize clusterer
      * @param in Input stream containing the seralized clusterer
      * @return The deserialized clusterer
      */
    override def deSerializeClusterer(in: ObjectInputStream): Clusterer

    /** Create classifier
      * @param name Name of the classifier
      * @param spec Classifier specification
      * @param targetColIndex Index of the target column in the training data
      * @param numDistinctValuesMap Distinct values for each column, if known
      * @param tableResult Training data
      * @return Created classifier
      */
    def createClassifier(
        name: String,
        spec: Specification,
        targetColIndex: Int,
        numDistinctValuesMap: java.util.Map[ColRef, Int],
        tableResult: TableResult
    ): Classifier

    /** Create classifier (Scala)
      * @param name Name of the classifier
      * @param specOpt Classifier specification
      * @param targetColIndex Index of the target column in the training data
      * @param numDistinctValuesMap Distinct values for each column, if known
      * @param tableResult Training data
      * @return Created classifier
      */
    override def createClassifier(
        name: String,
        specOpt: Option[(String, String)],
        colIndex: Int,
        numDistinctValuesMap: Map[ColRef, Int],
        tableResult: ScalaTableResult
    ): Classifier = {
        val spec: Specification = specOpt match {
            case Some((k, v)) => new Specification(k, v)
            case None => null
        }

        val dvMap: java.util.Map[ColRef, Int] = new java.util.HashMap()
        numDistinctValuesMap.foreach { case (col, n) => dvMap.put(col, n) }

        createClassifier(name, spec, colIndex, dvMap, TableResult(tableResult))
    }

    /** Create clusterer
      * @param name Name of the clusterer
      * @param spec Clusterer specification
      * @param numDistinctValuesMap Distinct values for each column, if known
      * @param tableResult Training data
      * @return Created clusterer
      */
    def createClusterer(
        name: String,
        spec: Specification,
        numDistinctValuesMap: java.util.Map[ColRef, Int],
        tableResult: TableResult
    ): Clusterer

    /** Create clusterer (Scala)
      * @param name Name of the clusterer
      * @param specOpt Clusterer specification
      * @param numDistinctValuesMap Distinct values for each column, if known
      * @param tableResult Training data
      * @return Created clusterer
      */
    override def createClusterer(
        name: String,
        specOpt: Option[(String, String)],
        numDistinctValuesMap: Map[ColRef, Int],
        tableResult: ScalaTableResult
    ): Clusterer = {
        val spec: Specification = specOpt match {
            case Some((k, v)) => new Specification(k, v)
            case None => null
        }

        val dvMap: java.util.Map[ColRef, Int] = new java.util.HashMap()
        numDistinctValuesMap.foreach { case (col, n) => dvMap.put(col, n) }

        createClusterer(name, spec, dvMap, TableResult(tableResult))
    }
}

/** Machine learning operator specification */
class Specification(val algorithm: String, val params: String)
