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

package com.scleradb.java.util

import java.sql.{Time, Timestamp, Date, Blob, Clob}

import scala.jdk.CollectionConverters._

import com.scleradb.sql.expr._
import com.scleradb.sql.types.SqlType
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.ScalTableRow

/** Utility functions to create and query Scala objects for Sclera in Java */
object JavaUtil {
    /** The Integer value represented by an IntConst object */
    def value(x: IntConst): Int = x.value
    /** The Short value represented by a ShortConst object */
    def value(x: ShortConst): Short = x.value
    /** The Long value represented by a LongConst object */
    def value(x: LongConst): Long = x.value
    /** The Float value represented by a FloatConst object */
    def value(x: FloatConst): Float = x.value
    /** The Double value represented by a DoubleConst object */
    def value(x: DoubleConst): Double = x.value
    /** The Boolean value represented by a BoolConst object */
    def value(x: BoolConst): Boolean = x.value
    /** The String value represented by a CharConst object */
    def value(x: CharConst): String = x.value
    /** The Date value represented by a DateConst object */
    def value(x: DateConst): Date = x.value
    /** The Timestamp value represented by a TimestampConst object */
    def value(x: TimestampConst): Timestamp = x.value
    /** The Time value represented by a TimeConst object */
    def value(x: TimeConst): Time = x.value
    /** The Blob value represented by a BlobConst object */
    def value(x: BlobConst): Blob = x.value
    /** The Clob value represented by a ClobConst object */
    def value(x: ClobConst): Clob = x.value

    /** Converts an Int to an IntConst object */
    def scalValue(v: Int): IntConst =
        Option(v).map { x => IntConst(x); } getOrElse null
    /** Converts a Short to a ShortConst object */
    def scalValue(v: Short): ShortConst =
        Option(v).map { x => ShortConst(x); } getOrElse null
    /** Converts a Long to a LongConst object */
    def scalValue(v: Long): LongConst =
        Option(v).map { x => LongConst(x); } getOrElse null
    /** Converts a Float to a FloatConst object */
    def scalValue(v: Float): FloatConst =
        Option(v).map { x => FloatConst(x); } getOrElse null
    /** Converts a Double to a DoubleConst object */
    def scalValue(v: Double): DoubleConst =
        Option(v).map { x => DoubleConst(x); } getOrElse null
    /** Converts a Boolean to a BoolConst object */
    def scalValue(v: Boolean): BoolConst =
        Option(v).map { x => BoolConst(x); } getOrElse null
    /** Converts a String to a CharConst object */
    def scalValue(v: String): CharConst =
        Option(v).map { x => CharConst(x); } getOrElse null
    /** Converts a Date to a DateConst object */
    def scalValue(v: Date): DateConst =
        Option(v).map { x => DateConst(x); } getOrElse null
    /** Converts a Timestamp to a TimestampConst object */
    def scalValue(v: Timestamp): TimestampConst =
        Option(v).map { x => TimestampConst(x); } getOrElse null
    /** Converts a Time to a TimeConst object */
    def scalValue(v: Time): TimeConst =
        Option(v).map { x => TimeConst(x); } getOrElse null
    /** Converts a Blob to a BlobConst object */
    def scalValue(v: Blob): BlobConst =
        Option(v).map { x => BlobConst(x); } getOrElse null
    /** Converts a Clob to a ClobConst object */
    def scalValue(v: Clob): ClobConst =
        Option(v).map { x => ClobConst(x); } getOrElse null

    /** Creates a SqlType object corresponding to the SQL code
      *
      * @param sqlTypeCode SQL type code (see java.sql.Types)
      * @return SqlType object for the type code
      */
    def sqlType(sqlTypeCode: Int): SqlType = SqlType(sqlTypeCode)
    /** Creates a SqlType object corresponding to the SQL code
      * and with given length
      *
      * @param sqlTypeCode SQL type code (see java.sql.Types)
      * @param length Length specification for the type
      * @return SqlType object
      */
    def sqlCharType(sqlTypeCode: Int, length: Int): SqlType =
        SqlType(sqlTypeCode, Option(length))
    /** Creates a SqlType object corresponding to the SQL code
      * and with given precision
      *
      * @param sqlTypeCode SQL type code (see java.sql.Types)
      * @param prec Precision specification for the type
      * @return SqlType object
      */
    def sqlType(sqlTypeCode: Int, prec: Int): SqlType =
        SqlType(sqlTypeCode, None, Option(prec), None)
    /** Creates a SqlType object corresponding to the SQL code
      * and with given precision and scale
      *
      * @param sqlTypeCode SQL type code (see java.sql.Types)
      * @param prec Precision specification for the type
      * @param scale Scale specification for the type
      * @return SqlType object
      */
    def sqlType(sqlTypeCode: Int, prec: Int, scale: Int): SqlType =
        SqlType(sqlTypeCode, None, Option(prec), Option(scale))

    /** Creates a Column object with the given name and type
      *
      * @param colName Column name
      * @param sqlType Type of the column
      * @return Column object
      */
    def column(colName: String, sqlType: SqlType): Column =
        Column(colName, sqlType)

    /** Creates a SortExpr object
      * with the given column name, direction and nulls order
      *
      * @param colName Column name
      * @param dir Sort direction object
      * @param nullsOrder Nulls order object
      * @return SortExpr object
      */
    private def sortExpr(
        colName: String,
        dir: SortDir,
        nullsOrder: NullsOrder
    ): SortExpr = SortExpr(ColRef(colName), dir, nullsOrder)
    /** Creates a SortExpr object with the given column name and sort direction
      *
      * @param colName Column name
      * @param dirStr Sort direction ("ASC" or "DESC")
      * @return SortExpr object
      */
    def sortExpr(colName: String, dirStr: String): SortExpr = {
        val dir: SortDir = sortDir(dirStr)
        sortExpr(colName, dir, dir.defaultNullsOrder)
    }
    /** Creates a SortExpr object with the given column name, sort direction and
      * nulls ordering
      *
      * @param colName Column name
      * @param dirStr Sort direction ("ASC" or "DESC")
      * @param nullsOrder Nulls ordering ("NULLSFIRST" or "NULLSLAST")
      * @return SortExpr object
      */
    def sortExpr(
        colName: String,
        dirStr: String,
        nullsOrderStr: String
    ): SortExpr = sortExpr(colName, sortDir(dirStr), nullsOrder(nullsOrderStr))

    /** Creates a SortDir object for the given specification
      *
      * @param dirStr Sort direction ("ASC" or "DESC")
      * @return SortDir object
      */
    private def sortDir(dirStr: String): SortDir =
        dirStr.toUpperCase match {
            case "ASC" => SortAsc
            case "DESC" => SortDesc
            case _ =>
                throw new IllegalArgumentException(
                    "Illegal sort direction: " + dirStr
                )
        }

    /** Creates a NullsOrder object for the given specification
      *
      * @param nullsOrder Nulls ordering ("NULLSFIRST" or "NULLSLAST")
      * @return NullsOrder object
      */
    private def nullsOrder(nullsOrderStr: String): NullsOrder =
        nullsOrderStr.toUpperCase match {
            case "NULLSFIRST" => NullsFirst
            case "NULLSLAST" => NullsLast
            case _ =>
                throw new IllegalArgumentException(
                    "Illegal nulls ordering: " + nullsOrderStr
                )
        }

    /** Create a table row object given the column values
      *
      * @param colVals Mapping of column names to values
      * @return Table row object
      */
    def tableRow(colVals: java.util.Map[String, ScalColValue]): ScalTableRow = 
        ScalTableRow(colVals.asScala.toMap)
}
