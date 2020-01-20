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

package com.scleradb.analytics.ml.classifier.objects

import com.scleradb.exec.Schema

import com.scleradb.sql.expr.{ColRef, ScalValueBase}
import com.scleradb.sql.result.{TableResult, TableRow}

import com.scleradb.analytics.ml.datatypes._
import com.scleradb.analytics.ml.service.MLService
import com.scleradb.analytics.ml.objects.{MLObjectId, MLObject, SchemaMLObject}

import com.scleradb.analytics.ml.classifier.datatypes.ClassifyResult

/** Abstract base class for classifier libraries */
abstract class Classifier extends MLObject {
    private[scleradb]
    override def typeStr: String = Classifier.typeStr

    /** Classifier name */
    override val name: String
    /** Target attribute */
    val targetAttr: DataAttribute
    /** Feature attributes */
    val featureAttrs: List[DataAttribute]

    /** Classify a row
      * @param t Table row to be classified;
      *        must contain the feature attribute columns
      * @return Assigned class label, if classification successful
      */
    def classifyOpt(t: TableRow): Option[ScalValueBase]

    /** Classify a result set of rows
      * @param rs Table result containing the rows to be classified;
      *           schema must contain the feature attribute columns
      * @return A modified tableresult, where each row has an additional column
      *         `targetColRef` containing the row's assigned class label,
      *         if classification successful
      */
    def classify(rs: TableResult, targetColRef: ColRef): TableResult =
        new ClassifyResult(rs, targetColRef, this)

    /** Classifier description */
    override def description: String

    private[scleradb]
    override def schemaObject: SchemaClassifier = SchemaClassifier(this)
}

private[scleradb]
object Classifier {
    val typeStr: String = "CLASSIFIER"

    def apply(
        serviceIdOpt: Option[String],
        name: String,
        specOpt: Option[(String, String)],
        targetColIndex: Int,
        numDistinctValuesMap: Map[ColRef, Int],
        tableResult: TableResult
    ): Classifier = MLService(serviceIdOpt).createClassifier(
        name, specOpt, targetColIndex, numDistinctValuesMap, tableResult
    )
}

private[scleradb]
class SchemaClassifier(override val obj: Classifier) extends SchemaMLObject

private[scleradb]
object SchemaClassifier {
    def apply(classifier: Classifier): SchemaClassifier =
        new SchemaClassifier(classifier)

    def objectOpt(schema: Schema, id: MLObjectId): Option[SchemaClassifier] =
        objectOpt(schema, id.repr)

    def objectOpt(schema: Schema, id: String): Option[SchemaClassifier] =
        SchemaMLObject.objectOpt(schema, id) match {
            case Some(obj: SchemaClassifier) => Some(obj)
            case _ => None
        }

    def objects(schema: Schema): List[SchemaClassifier] =
        SchemaMLObject.objects(schema).flatMap {
            case (obj: SchemaClassifier) => Some(obj)
            case _ => None
        }
}
