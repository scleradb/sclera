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

package com.scleradb.analytics.ml.imputer.expr

import com.scleradb.sql.expr.{RelExpr, ColRef}

import com.scleradb.analytics.ml.imputer.datatypes.ImputeSpec
import com.scleradb.analytics.ml.expr.MLRelOp

private[scleradb]
case class Impute(
    imputeSpecs: List[ImputeSpec]
) extends MLRelOp {
    override val arity: Int = 1

    override def tableColRefs(inputs: List[RelExpr]): List[ColRef] = {
        val specMap: Map[String, ImputeSpec] = Map() ++
            imputeSpecs.map { spec => (spec.imputeColRef.name -> spec) }

        inputs.head.tableColRefs.flatMap { colRef =>
            specMap.get(colRef.name) match {
                case Some(spec) => spec.flagColRefOpt match {
                    case Some(fColRef) =>
                        List(colRef, fColRef)
                    case None => List(colRef)
                }

                case None => List(colRef)
            }
        }
    }

    def addedCols: List[ColRef] = imputeSpecs.flatMap { spec => spec.addedCols }
}
