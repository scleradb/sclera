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

package com.scleradb.sql.result

import java.sql.{Time, Timestamp, Date, Blob, Clob}

import com.scleradb.sql.types._
import com.scleradb.sql.expr._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.exec.ScalCastEvaluator

/** Abstract base class for table result rows
  *
  * Encapsulates a row of an intermediate result.
  * Contains the accessor functions for the included data values.
  */
abstract class TableRow {
    /** Retrieve contents of a column as a String
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[String] object
      *          (non-null value returned as Some[String], null as None)
      */
    def getStringOpt(cname: String): Option[String]
    /** Retrieve contents of a column as an Int
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Int.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Int] object
      *          (non-null value returned as Some[Int], null as None)
      */
    def getIntOpt(cname: String): Option[Int]
    /** Retrieve contents of a column as a Short
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Short.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Short] object
      *          (non-null value returned as Some[Short], null as None)
      */
    def getShortOpt(cname: String): Option[Short]
    /** Retrieve contents of a column as a Long
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Long.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Long] object
      *          (non-null value returned as Some[Long], null as None)
      */
    def getLongOpt(cname: String): Option[Long]
    /** Retrieve contents of a column as a Float
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Float.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Float] object
      *          (non-null value returned as Some[Float], null as None)
      */
    def getFloatOpt(cname: String): Option[Float]
    /** Retrieve contents of a column as a Double
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Double.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Double] object
      *          (non-null value returned as Some[Double], null as None)
      */
    def getDoubleOpt(cname: String): Option[Double]
    /** Retrieve contents of a column as a Boolean
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Boolean.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Boolean] object
      *          (non-null value returned as Some[Boolean], null as None)
      */
    def getBooleanOpt(cname: String): Option[Boolean]
    /** Retrieve contents of a column as a Date
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Date.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Date] object
      *          (non-null value returned as Some[Date], null as None)
      */
    def getDateOpt(cname: String): Option[Date]
    /** Retrieve contents of a column as a Time
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Time.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Time] object
      *          (non-null value returned as Some[Time], null as None)
      */
    def getTimeOpt(cname: String): Option[Time]
    /** Retrieve contents of a column as a Timestamp
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Timestamp.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Timestamp] object
      *          (non-null value returned as Some[Timestamp], null as None)
      */
    def getTimestampOpt(cname: String): Option[Timestamp]
    /** Retrieve contents of a column as a Blob
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Blob.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Blob] object
      *          (non-null value returned as Some[Blob], null as None)
      */
    def getBlobOpt(cname: String): Option[Blob]
    /** Retrieve contents of a column as a Clob
      *
      * Throws IllegalArgumentException if the column does not exist, or if
      * the contents cannot be converted to Clob.
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[Clob] object
      *          (non-null value returned as Some[Clob], null as None)
      */
    def getClobOpt(cname: String): Option[Clob]

    /** Retrieve contents of a column as a scalar value expression
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param cname Column name (must be present in the the schema)
      * @param sqlType Specifies the SQL type of the scalar value expression
      * @return Contents of the column as an Option[ScalValueBase] object
      *          (non-null value returned as Some[ScalValueBase], null as None)
      */
    def getScalValueOpt(
        cname: String,
        sqlType: SqlType
    ): Option[ScalValueBase] = sqlType match {
        case SqlInteger => getIntOpt(cname).map { v => IntConst(v) }
        case SqlSmallInt => getShortOpt(cname).map { v => ShortConst(v) }
        case SqlBigInt => getLongOpt(cname).map { v => LongConst(v) }
        case (_: SqlSinglePrecFloatingPoint) =>
            getFloatOpt(cname).map { v => FloatConst(v) }
        case (_: SqlDoublePrecFloatingPoint) =>
            getDoubleOpt(cname).map { v => DoubleConst(v) }
        case SqlBool => getBooleanOpt(cname).map { v => BoolConst(v) }
        case (_: SqlChar) => getStringOpt(cname).map { v => CharConst(v) }
        case SqlTimestamp =>
            getTimestampOpt(cname).map { v => TimestampConst(v) }
        case SqlTime => getTimeOpt(cname).map { v => TimeConst(v) }
        case SqlDate => getDateOpt(cname).map { v => DateConst(v) }
        case SqlBlob => getBlobOpt(cname).map { v => BlobConst(v) }
        case SqlClob => getClobOpt(cname).map { v => ClobConst(v) }
        case SqlOption(baseType) => getScalValueOpt(cname, baseType)
        case _ => throw new RuntimeException("Cannot translate: " + sqlType)
    }

    /** Retrieve contents of a column as a scalar value expression
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param cname Column name (must be present in the the schema)
      * @param sqlType Specifies the SQL type of the scalar value expression
      * @return Contents of the column as a ScalColValue object
      */
    def getScalExpr(
        cname: String,
        sqlType: SqlType
    ): ScalColValue = getScalValueOpt(cname, sqlType) getOrElse SqlNull(sqlType)

    /** Retrieve contents of a column as a scalar value expression
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param col Column object (must be present in the the schema)
      * @return Contents of the column as a ScalColValue object
      */
    def getScalExpr(col: Column): ScalColValue =
        getScalExpr(col.name, col.sqlType)

    /** Retrieve contents of multiple columns as scalar value expressions
      *
      * Throws IllegalArgumentException if any of the columns does not exist
      * @param cols List of Column objects
      *             (each column must be present in the the schema)
      * @return Mapping of column names to the contents of each column
      *          encoded as a ScalColValue object
      */
    def getScalExprMap(
        cols: List[Column]
    ): Map[String, ScalColValue] =
        Map() ++ cols.map { col => (col.name -> getScalExpr(col)) }

    /** Is the column null in this row?
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param cname Column name (must be present in the the schema)
      * @return true if the column is null in this row, false otherwise
      */
    def isNull(cname: String): Boolean = getStringOpt(cname).isEmpty

    /** Is the column null in this row?
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param col Column object (must be present in the the schema)
      * @return true if the column is null in this row, false otherwise
      */
    def isNull(col: Column): Boolean = isNull(col.name)

    /** Is the column null in this row?
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param col Column reference (must be present in the the schema)
      * @return true if the column is null in this row, false otherwise
      */
    def isNull(col: ColRef): Boolean = isNull(col.name)
}

/** Abstract base class for table result rows which
  * include the column type information in addition to the column values.
  *
  * Encapsulates a row of an intermediate result.
  * Contains the accessor functions for the included data values.
  */
abstract class ScalTableRow extends TableRow {
    /** Retrieve contents of a column as a scalar value expression, if any
      *
      * @param cname Column name
      * @return Contents of the column as an Option[ScalColValue] object,
      *         which is None when cname is not present in the schema
      */
    def getScalExprOpt(cname: String): Option[ScalColValue]

    /** Retrieve contents of a column as a scalar value expression, if any
      *
      * @param colRef Column reference
      * @return Contents of the column as an Option[ScalColValue] object,
      *         which is None when colRef is not present in the schema
      */
    def getScalExprOpt(colRef: ColRef): Option[ScalColValue] =
        getScalExprOpt(colRef.name)

    /** Retrieve contents of a column as a scalar value expression
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as a ScalColValue object
      */
    def getScalExpr(cname: String): ScalColValue =
        getScalExprOpt(cname) getOrElse {
            throw new IllegalArgumentException(
                "Column \"" + cname + "\" not found"
            )
        }

    /** Retrieve contents of a column as a scalar value expression
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param colRef Column reference (must be present in the the schema)
      * @return Contents of the column as a ScalColValue object
      */
    def getScalExpr(colRef: ColRef): ScalColValue = getScalExpr(colRef.name)

    override def getStringOpt(cname: String): Option[String] =
        ScalCastEvaluator.valueAsStringOpt(getScalExpr(cname))

    override def getShortOpt(cname: String): Option[Short] =
        ScalCastEvaluator.valueAsShortOpt(getScalExpr(cname))

    override def getIntOpt(cname: String): Option[Int] =
        ScalCastEvaluator.valueAsIntOpt(getScalExpr(cname))

    override def getLongOpt(cname: String): Option[Long] =
        ScalCastEvaluator.valueAsLongOpt(getScalExpr(cname))

    override def getFloatOpt(cname: String): Option[Float] =
        ScalCastEvaluator.valueAsFloatOpt(getScalExpr(cname))

    override def getDoubleOpt(cname: String): Option[Double] =
        ScalCastEvaluator.valueAsDoubleOpt(getScalExpr(cname))

    override def getBooleanOpt(cname: String): Option[Boolean] =
        ScalCastEvaluator.valueAsBooleanOpt(getScalExpr(cname))

    override def getDateOpt(cname: String): Option[Date] =
        ScalCastEvaluator.valueAsDateOpt(getScalExpr(cname))

    override def getTimeOpt(cname: String): Option[Time] =
        ScalCastEvaluator.valueAsTimeOpt(getScalExpr(cname))

    override def getTimestampOpt(cname: String): Option[Timestamp] =
        ScalCastEvaluator.valueAsTimestampOpt(getScalExpr(cname))

    override def getBlobOpt(cname: String): Option[Blob] =
        ScalCastEvaluator.valueAsBlobOpt(getScalExpr(cname))

    override def getClobOpt(cname: String): Option[Clob] =
        ScalCastEvaluator.valueAsClobOpt(getScalExpr(cname))
}

/** Result row wrapper over column-name to scalar value expression maps
  *
  * @param row Column name to value expression map
  */
class ScalMapTableRow(
    row: Map[String, ScalColValue]
) extends ScalTableRow {
    override def getScalExprOpt(cname: String): Option[ScalColValue] =
        row.get(cname)

    override def toString: String = row.toString
}

/** Companion object containing constructors for ScalTableRow */
object ScalTableRow {
    /** Create a result row from a column name - value map */
    def apply(valueMap: Map[String, ScalColValue]): ScalTableRow =
        new ScalMapTableRow(valueMap)
    /** Create a result row from a column name - value pairs */
    def apply(values: (String, ScalColValue)*): ScalTableRow =
        apply(Map() ++ values)
    /** Create a result row from a column name - value list */
    def apply(values: Iterable[(String, ScalColValue)]): ScalTableRow =
        apply(Map() ++ values)
}

/** Abstract base class for base table rows
  *
  * Encapsulates a row of the content of a base table
  * Contains the accessor functions for the included data values.
  */
abstract class BaseTableRow extends TableRow {
    /** Retrieve contents of a column as a value of type T
      *
      * Throws IllegalArgumentException if the column does not exist
      * @param cname Column name (must be present in the the schema)
      * @return Contents of the column as an Option[T] object
      *          (non-null value returned as Some[T], null as None)
      */
    def getValueOpt(cname: String): Option[Any]

    override def getIntOpt(cname: String): Option[Int] =
        getValueOpt(cname).map {
            case (v: Int) => v
            case (v: Short) => v.toInt
            case (v: Long) => v.toInt
            case (v: Float) => v.toInt
            case (v: Double) => v.toInt
            case (v: java.math.BigDecimal) => v.intValue
            case (v: java.math.BigInteger) => v.intValue
            case v => v.toString.toInt
        }

    override def getShortOpt(cname: String): Option[Short] =
        getValueOpt(cname).map {
            case (v: Int) => v.toShort
            case (v: Short) => v
            case (v: Long) => v.toShort
            case (v: Float) => v.toShort
            case (v: Double) => v.toShort
            case (v: java.math.BigDecimal) => v.shortValue
            case (v: java.math.BigInteger) => v.shortValue
            case v => v.toString.toShort
        }

    override def getLongOpt(cname: String): Option[Long] =
        getValueOpt(cname).map {
            case (v: Int) => v.toLong
            case (v: Short) => v.toLong
            case (v: Long) => v
            case (v: Float) => v.toLong
            case (v: Double) => v.toLong
            case (v: java.math.BigDecimal) => v.longValue
            case (v: java.math.BigInteger) => v.longValue
            case v => v.toString.toLong
        }

    override def getFloatOpt(cname: String): Option[Float] =
        getValueOpt(cname).map {
            case (v: Int) => v.toFloat
            case (v: Short) => v.toFloat
            case (v: Long) => v.toFloat
            case (v: Float) => v
            case (v: Double) => v.toFloat
            case (v: java.math.BigDecimal) => v.floatValue
            case (v: java.math.BigInteger) => v.floatValue
            case v => v.toString.toFloat
        }

    override def getDoubleOpt(cname: String): Option[Double] =
        getValueOpt(cname).map {
            case (v: Int) => v.toDouble
            case (v: Short) => v.toDouble
            case (v: Long) => v.toDouble
            case (v: Float) => v.toDouble
            case (v: Double) => v
            case (v: java.math.BigDecimal) => v.doubleValue
            case (v: java.math.BigInteger) => v.doubleValue
            case v => v.toString.toDouble
        }

    // workaround for databases storing boolean as numeric
    override def getBooleanOpt(cname: String): Option[Boolean] =
        getStringOpt(cname).map {
            case "false" | "0" => false
            case _ => true
        }
}

/** Wraps over another table row, adding/replacing columns
  *
  * For columns present in both row and extRow,
  * the value in extRow takes precedence.
  *
  * @param row       Row to be masked, or extended with new columns
  * @param extRow    Extending / masking row
  */
class ExtendedTableRow(
    row: ScalTableRow,
    extRow: ScalTableRow
) extends ScalTableRow {
    override def getScalExprOpt(
        cname: String
    ): Option[ScalColValue] =
        extRow.getScalExprOpt(cname) orElse row.getScalExprOpt(cname)
}

/** Companion object containing constructor for ExtendedTableRow */
object ExtendedTableRow {
    def apply(
        row: ScalTableRow,
        extRow: ScalTableRow
    ): ExtendedTableRow = new ExtendedTableRow(row, extRow)

    def apply(
        row: ScalTableRow,
        extColMap: Map[String, ScalColValue]
    ): ExtendedTableRow = apply(row, ScalTableRow(extColMap))
}

/** Companion object containing constructor for ExtendedTableRow */
object NullExtendedTableRow {
    def apply(
        row: ScalTableRow,
        extTypeMap: Map[String, SqlType]
    ): ExtendedTableRow = {
        val nullRow: ScalTableRow = ScalTableRow(
            extTypeMap.view.mapValues { sqlType => SqlNull(sqlType) }
        )

        ExtendedTableRow(row, nullRow)
    }

    def apply(
        row: ScalTableRow,
        extCols: List[Column]
    ): ExtendedTableRow =
        apply(row, Map() ++ extCols.map { col => col.name -> col.sqlType })
}

/** Concatenates two rows.
  *
  * For columns present in both rows, the value in rowB takes precedence.
  *
  * @param rowA First row
  * @param rowB Second row.
  */
class ConcatTableRow(
    rowA: ScalTableRow,
    rowB: ScalTableRow
) extends ScalTableRow {
    override def getScalExprOpt(
        cname: String
    ): Option[ScalColValue] =
         rowA.getScalExprOpt(cname) orElse rowB.getScalExprOpt(cname)
}

/** Companion object containing constructor for ConcatTableRow */
object ConcatTableRow {
    def apply(
        rowA: ScalTableRow,
        rowB: ScalTableRow
    ): ConcatTableRow = new ConcatTableRow(rowA, rowB)
}
