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

package com.scleradb.dbms.rdbms.driver

import java.sql.{Date, Time, Timestamp, Blob, Clob}

import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.BaseTableRow
import com.scleradb.sql.exec.ScalCastEvaluator

private[scleradb]
class SqlTableRow(
    valMap: Map[String, Option[java.lang.Object]],
    val cols: List[Column]
) extends BaseTableRow {
    override def getValueOpt(cname: String): Option[java.lang.Object] =
        valMap.get(cname.toUpperCase).getOrElse {
            throw new IllegalArgumentException(
                "Column \"" + cname + "\" not found"
            )
        }

    override def getStringOpt(cname: String): Option[String] =
        getValueOpt(cname).map { v => v.toString }

    override def getDateOpt(cname: String): Option[Date] =
        getValueOpt(cname).map {
            case (v: Date) => v
            case (v: java.util.Date) => ScalCastEvaluator.toDate(v)
            case v => ScalCastEvaluator.toDate(v.toString)
        }

    override def getTimeOpt(cname: String): Option[Time] =
        getValueOpt(cname).map {
            case (v: Time) => v
            case (v: java.util.Date) => ScalCastEvaluator.toTime(v)
            case v => ScalCastEvaluator.toTime(v.toString)
        }

    override def getTimestampOpt(cname: String): Option[Timestamp] =
        getValueOpt(cname).map {
            case (v: Timestamp) => v
            case (v: java.util.Date) => ScalCastEvaluator.toTimestamp(v)
            case v => ScalCastEvaluator.toTimestamp(v.toString)
        }

    override def getBlobOpt(cname: String): Option[Blob] =
        getValueOpt(cname).map {
            case (v: Blob) => v
            case v =>
                throw new IllegalArgumentException(
                    "Found object of type " + v.getClass.getName +
                    ", expecting java.sql.Blob"
                )
        }

    override def getClobOpt(cname: String): Option[Clob] =
        getValueOpt(cname).map {
            case (v: Clob) => v
            case v =>
                throw new IllegalArgumentException(
                    "Found object of type " + v.getClass.getName +
                    ", expecting java.sql.Clob"
                )
        }
}

private[scleradb]
object SqlTableRow {
    def apply(
        valMap: Map[String, Option[java.lang.Object]],
        cols: List[Column]
    ): SqlTableRow =
        new SqlTableRow(valMap, cols)
}
