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

package com.scleradb.sql.exec

import java.sql.{Time, Timestamp, Date}

import scala.util.Try

import com.scleradb.sql.types._
import com.scleradb.sql.expr._

private[scleradb]
object ScalTypeInferer {
    private val inferInt: SqlType = SqlInteger
    private val inferLong: SqlType = SqlBigInt
    private val inferDouble: SqlType = SqlFloat(None)
    private val inferTimestamp: SqlType = SqlTimestamp
    private val inferDate: SqlType = SqlDate
    private val inferTime: SqlType = SqlTime
    private val inferChar: SqlType = SqlCharVarying(None)

    def inferType(vs: List[String]): SqlType = if( vs.isEmpty ) inferChar else {
        vs.tail.foldLeft (inferType(vs.head.trim)) { case (t, v) =>
            weakerType(t, inferType(v.trim))
        }
    }

    // infer the strongest type from the given string contents
    private def inferType(v: String): SqlType = {
        if( Try(v.toInt).isSuccess )
            inferInt
        else if( Try(v.toLong).isSuccess )
            inferLong
        else if( Try(v.toDouble).isSuccess )
            inferDouble
        else if( Try(Timestamp.valueOf(v)).isSuccess )
            inferTimestamp
        else if( Try(Date.valueOf(v)).isSuccess )
            inferDate
        else if( Try(Time.valueOf(v)).isSuccess )
            inferTime
        else inferChar
    }

    // identift the weaker of the two given types
    private def weakerType(ta: SqlType, tb: SqlType): SqlType =
        if( ta == tb ) ta else {
            (ta, tb) match {
                case (t: SqlCharVarying, _) => t
                case (_, t: SqlCharVarying) => t

                case (t@SqlBigInt, SqlInteger) => t
                case (SqlInteger, t@SqlBigInt) => t

                case (t: SqlFloat, SqlInteger) => t
                case (SqlInteger, t: SqlFloat) => t

                case (t: SqlFloat, SqlBigInt) => t
                case (SqlBigInt, t: SqlFloat) => t

                case (t@SqlDate, SqlTimestamp) => t
                case (SqlTimestamp, t@SqlDate) => t

                case (t@SqlTime, SqlTimestamp) => t
                case (SqlTimestamp, t@SqlTime) => t

                case _ => inferChar
            }
        }
}
