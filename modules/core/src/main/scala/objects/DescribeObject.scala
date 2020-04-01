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

package com.scleradb.objects

import com.scleradb.sql.result.TableResult
import com.scleradb.sql.datatypes._
import com.scleradb.sql.statements._
import com.scleradb.sql.types.SqlOption
import com.scleradb.sql.objects._

import com.scleradb.sql.mapper.default.{ScleraSQLMapper => SqlMapper}

import com.scleradb.analytics.ml.classifier.objects._
import com.scleradb.analytics.ml.clusterer.objects._

private[scleradb]
object DescribeObject {
    def describe(
        obj: SchemaObject,
        duration: DbObjectDuration
    ): List[String] = obj match {
        case (sv: SchemaView) =>
            val view: View = sv.obj
            val nameStr: String = duration match {
                case Temporary => "[TEMPORARY] VIEW " + view.name
                case Persistent => "VIEW " + view.name
            }

            val queryStr: String =
                try SqlMapper.queryString(SqlRelQueryStatement(view.expr))
                catch {
                    case (_: Throwable) => "[NON-RELATIONAL VIEW]"
                }

            List(nameStr, queryStr)

        case (st: SchemaTable) =>
            val qualname: String = st.id.repr
            val nameStr: String = duration match {
                case Temporary => "[TEMPORARY] TABLE " + qualname
                case Persistent => "TABLE " + qualname
            }

            val table: Table = st.obj
            val colStrs: List[String] = table.columns.map {
                case Column(colName, colType, familyOpt) =>
                    val colTypeStr: String = colType match {
                        case SqlOption(t) => t.repr
                        case t => t.repr + " NOT NULL"
                    }

                    val familyStr: String =
                        familyOpt.map(f => f + ":").getOrElse("")

                    "  " + familyStr + colName + " " + colTypeStr
            }

            val keyStrs: List[String] = table.keyOpt.toList.map {
                case PrimaryKey(cols) =>
                    "PRIMARY KEY (" +
                        cols.map(col => col.repr).mkString(", ") +
                    ")"
            }

            val refStrs: List[String] = table.foreignKeys.map {
                case ForeignKey(cols, refLocIdOpt, refTableName, refCols) =>
                    val refTableIdStr: String = refLocIdOpt match {
                        case Some(refLocId) =>
                            TableId(refLocId, refTableName).repr
                        case None => refTableName
                    }

                    val refColsStr: String =
                        if( refCols.isEmpty ) "" else
                        "(" + refCols.map(col => col.name).mkString(", ") + ")"

                    "FOREIGN KEY (" +
                        cols.map(col => col.repr).mkString(", ") +
                    ") REFERENCES " + refTableIdStr + refColsStr
            }

            nameStr::(colStrs:::keyStrs:::refStrs)

        case (sc: SchemaClassifier) =>
            val classifier: Classifier = sc.obj
            val nameStr: String = duration match {
                case Temporary => "[TEMPORARY] CLASSIFIER " + classifier.name
                case Persistent => "CLASSIFIER " + classifier.name
            }

            val targetAttrStr: String = " " + classifier.targetAttr.toString
            val featureAttrStrs: List[String] = classifier.featureAttrs.map {
                attr => " " + attr.toString
            }

            (nameStr::"TARGET"::targetAttrStr::"FEATURES"::featureAttrStrs):::
            List("", classifier.description)

        case (sc: SchemaClusterer) =>
            val clusterer: Clusterer = sc.obj
            val nameStr: String = duration match {
                case Temporary => "[TEMPORARY] CLUSTERER " + clusterer.name
                case Persistent => "CLUSTERER " + clusterer.name
            }

            val attrStrs: List[String] = clusterer.attrs.map {
                attr => "  " + attr.name + " " + attr.sqlType.repr
            }

            (nameStr::"FEATURES"::attrStrs):::
            List("", clusterer.description)

        case _ =>
            throw new IllegalArgumentException(
                "Describe not implemented for " + obj.id.repr
            )
    }
}
