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

package com.scleradb.sql.types

import java.sql.Types
import java.sql.{Time, Timestamp, Date, Blob, Clob}

import com.scleradb.api._
import com.scleradb.sql.mapper.default.{ScleraSQLMapper => SqlMapper}

/** Abstract base class for all SQL types */
sealed abstract class SqlType {
    /** Scala type corresponding to this SQL type */
    type ScalaType
    /** Scala type corresponding to the underlying "NOT NULL" SQL type */
    type ScalaBaseType

    /** DbStruct object corresponding to this SQL type */
    def dbStruct(name: String): DbStruct[ScalaType]
    /** The underlying "NOT NULL" SQL type */
    def baseType: SqlBaseType[ScalaBaseType]
    /** The nullable SQL type */
    def option: SqlOption[ScalaBaseType]
    /** Is this type a nullable SQL type? */
    def isOption: Boolean

    /** Relaxation of this type */
    def relaxedType: SqlType

    /** The SQL type code associated with this SQL type */
    def sqlTypeCode: Int
    /** The Java class associated with this SQL type */
    def javaClass: java.lang.Class[_]

    /** The string representation of this SQL type */
    def repr: String = SqlMapper.sqlTypeString(this)
}

/** Parameterized abstract base class for all SQL types */
sealed abstract class SqlTypeRoot[T] extends SqlType {
    type ScalaType = T
    override def dbStruct(name: String): DbColStruct[T]
}

/** Parameterized abstract base class for all "NOT NULL" SQL types */
sealed abstract class SqlBaseType[T] extends SqlTypeRoot[T] {
    type ScalaBaseType = T
    override def dbStruct(name: String): DbBaseTypeColStruct[T]
    override def baseType: SqlBaseType[T] = this
    override def option: SqlOption[T] = SqlOption[T](this)
    override def isOption: Boolean = false
}

/** Numeric trait */
trait SqlNumeric extends SqlType {
    override def relaxedType: SqlType = SqlFloat(None)
}

/** SQL type for INTEGER */
case object SqlInteger extends SqlBaseType[Int] with SqlNumeric {
    override def dbStruct(name: String): IntStruct = IntStruct(name)

    override def sqlTypeCode: Int = Types.INTEGER
    override def javaClass: java.lang.Class[Int] = classOf[Int]
}

/** SQL type for SMALLINT */
case object SqlSmallInt extends SqlBaseType[Short] with SqlNumeric {
    override def dbStruct(name: String): ShortStruct = ShortStruct(name)

    override def sqlTypeCode: Int = Types.SMALLINT
    override def javaClass: java.lang.Class[Short] = classOf[Short]
}

/** SQL type for BIGINT */
case object SqlBigInt extends SqlBaseType[Long] with SqlNumeric {
    override def dbStruct(name: String): LongStruct = LongStruct(name)

    override def sqlTypeCode: Int = Types.BIGINT
    override def javaClass: java.lang.Class[Long] = classOf[Long]
}

/** Abstract base class for SQL single-precision floating point */
sealed abstract class SqlSinglePrecFloatingPoint extends
SqlBaseType[Float] with SqlNumeric {
    override def dbStruct(name: String): FloatStruct = FloatStruct(name)

    override def javaClass: java.lang.Class[Float] = classOf[Float]
}

/** SQL type for REAL */
case object SqlReal extends SqlSinglePrecFloatingPoint {
    override def sqlTypeCode: Int = Types.REAL
}

/** Abstract base class for SQL double-precision floating point */
sealed abstract class SqlDoublePrecFloatingPoint extends
SqlBaseType[Double] with SqlNumeric {
    override def dbStruct(name: String): DoubleStruct = DoubleStruct(name)

    override def javaClass: java.lang.Class[Double] = classOf[Double]
}

/** SQL type for DECIMAL
  *
  * @param precOpt precision (optional)
  * @param scaleOpt scale (optional)
  */
case class SqlDecimal(
    precOpt: Option[Int],
    scaleOpt: Option[Int]
) extends SqlDoublePrecFloatingPoint {
    override def sqlTypeCode: Int = Types.DECIMAL
}

/** SQL type for FLOAT
  *
  * @param precOpt precision (optional)
  */
case class SqlFloat(precOpt: Option[Int]) extends SqlDoublePrecFloatingPoint {
    override def sqlTypeCode: Int = Types.FLOAT
}

/** SQL type for BOOLEAN */
case object SqlBool extends SqlBaseType[Boolean] {
    override def dbStruct(name: String): BoolStruct = BoolStruct(name)

    override def relaxedType: SqlType = this
    override def sqlTypeCode: Int = Types.BOOLEAN
    override def javaClass: java.lang.Class[Boolean] = classOf[Boolean]
}

/** Abstract base class for SQL character string */
sealed abstract class SqlChar extends SqlBaseType[String] {
    override def dbStruct(name: String): StringStruct = StringStruct(name)

    override def relaxedType: SqlType = SqlText
    override def javaClass: java.lang.Class[String] = classOf[String]
}

/** SQL type for CHAR 
  *
  * @param lenOpt length (optional)
  */
case class SqlCharFixed(lenOpt: Option[Int]) extends SqlChar {
    override def sqlTypeCode: Int = Types.CHAR
}

/** SQL type for VARCHAR
  *
  * @param lenOpt length (optional)
  */
case class SqlCharVarying(lenOpt: Option[Int]) extends SqlChar {
    override def sqlTypeCode: Int = Types.VARCHAR
}

/** SQL type for LONGVARCHAR */
case object SqlText extends SqlChar {
    override def sqlTypeCode: Int = Types.LONGVARCHAR
}
/** Date-time trait */
trait SqlDateTime extends SqlType {
    override def relaxedType: SqlType = SqlTimestamp
}

/** SQL type for TIMESTAMP */
case object SqlTimestamp extends SqlBaseType[Timestamp] with SqlDateTime {
    override def dbStruct(name: String): TimestampStruct = TimestampStruct(name)

    override def sqlTypeCode: Int = Types.TIMESTAMP
    override def javaClass: java.lang.Class[Timestamp] = classOf[Timestamp]
}

/** SQL type for TIME */
case object SqlTime extends SqlBaseType[Time] with SqlDateTime {
    override def dbStruct(name: String): TimeStruct = TimeStruct(name)

    override def sqlTypeCode: Int = Types.TIME
    override def javaClass: java.lang.Class[Time] = classOf[Time]
}

/** SQL type for DATE */
case object SqlDate extends SqlBaseType[Date] with SqlDateTime {
    override def dbStruct(name: String): DateStruct = DateStruct(name)

    override def sqlTypeCode: Int = Types.DATE
    override def javaClass: java.lang.Class[Date] = classOf[Date]
}

/** SQL type for BLOB */
case object SqlBlob extends SqlBaseType[Blob] {
    override def dbStruct(name: String): BlobStruct = BlobStruct(name)

    override def relaxedType: SqlType = this
    override def sqlTypeCode: Int = Types.BLOB
    override def javaClass: java.lang.Class[Blob] = classOf[Blob]
}

/** SQL type for CLOB */
case object SqlClob extends SqlBaseType[Clob] {
    override def dbStruct(name: String): ClobStruct = ClobStruct(name)

    override def relaxedType: SqlType = this
    override def sqlTypeCode: Int = Types.CLOB
    override def javaClass: java.lang.Class[Clob] = classOf[Clob]
}

/** Wrapper to convert a type to a nullable type
  *
  * @param baseType Underlying "NOT NULL" SQL type
  */
case class SqlOption[A](
    override val baseType: SqlBaseType[A]
) extends SqlTypeRoot[Option[A]] {
    type ScalaBaseType = A
    override def dbStruct(name: String): DbColStruct[Option[A]] =
        DbOptionStruct[A](baseType.dbStruct(name))

    override def option: SqlOption[A] = this
    override def isOption: Boolean = true

    override def relaxedType: SqlType = baseType.relaxedType.option
    override def sqlTypeCode: Int = baseType.sqlTypeCode
    override def javaClass: java.lang.Class[_] = baseType.javaClass
}

/** Dummy type for "NULL" */
case object SqlNullType extends SqlBaseType[Null] {
    override def dbStruct(name: String): DbBaseTypeColStruct[Null] = NullStruct

    override def relaxedType: SqlType = this
    override def sqlTypeCode: Int = Types.NULL
    override def javaClass: java.lang.Class[Null] = classOf[Null]
}

/** Companion object - contains properties, constructor and helper functions */
object SqlType {
    /** Maximum length of a VARCHAR. Default VARCHAR length when unspecified */
    val maxVarCharLen: Int = 767

    /** Maximum length, to detect spurious values returned by JDBC drivers */
    val maxLen: Int = 767
    /** Maximum precision, to detect spurious values returned by JDBC drivers */
    val maxPrec: Int = 767
    /** Maximum scale, to detect spurious values returned by JDBC drivers */
    val maxScale: Int = 767

    /** Create the SqlType object given the specifications
      *
      * @param colType SQL type code
      * @param lenOpt Length (optional)
      * @param precOpt Precision (optional)
      * @param scaleOpt Scale (optional)
      * @param classNameOpt Associated Java class name (optional)
      */
    def apply(
        colType: Int,
        lenOpt: Option[Int] = None,
        precOpt: Option[Int] = None,
        scaleOpt: Option[Int] = None,
        classNameOpt: Option[String] = None
    ): SqlType = colType match {
        case Types.BIGINT => SqlBigInt
        case Types.BIT => SqlBool // JDBC quirk
        case Types.BOOLEAN => SqlBool
        case Types.CHAR => SqlCharFixed(lenOpt)
        case Types.DATE => SqlDate
        case (Types.DECIMAL | Types.NUMERIC) =>
            (precOpt, scaleOpt) match {
                case (Some(prec), Some(0)) if( prec <= 5 ) => SqlSmallInt
                case (Some(prec), Some(0)) if( prec <= 10 ) => SqlInteger
                case (_, Some(0)) =>
                    if( classNameOpt == Some("java.math.BigDecimal") )
                        SqlFloat(precOpt)
                    else SqlBigInt
                case _ => SqlDecimal(precOpt, scaleOpt)
            }
        case Types.DOUBLE => SqlFloat(precOpt)
        case Types.FLOAT => SqlFloat(precOpt)
        case Types.INTEGER => SqlInteger
        case Types.NCHAR => SqlCharFixed(lenOpt)
        case Types.NULL => SqlNullType
        case Types.NVARCHAR =>
            SqlCharVarying(lenOpt orElse Some(maxVarCharLen))
        case Types.REAL => SqlReal
        case Types.SMALLINT => SqlSmallInt
        case Types.TIME => SqlTime
        case Types.TIMESTAMP => SqlTimestamp
        case Types.TINYINT => SqlSmallInt
        case Types.VARCHAR =>
            SqlCharVarying(lenOpt orElse Some(maxVarCharLen))
        case Types.LONGVARCHAR => SqlText
        case Types.BLOB => SqlBlob
        case Types.CLOB => SqlClob
        case _ => throw new IllegalArgumentException(
            "Type (code: " + colType + ") not supported"
        )
    }

    // The following are not included in the types as they are not well-defined

    /** Get the precision of a SQL type
      *
      * @param sqlType SQL type object
      * @return Some[Int] object containing precision of the type, if defined.
      *         None if precision is not defined for this type
      */
    def precisionOpt(sqlType: SqlType): Option[Int] = sqlType match {
        case SqlOption(t) => precisionOpt(t)
        case SqlBigInt => Some(19)
        case SqlCharFixed(lenOpt) => lenOpt orElse Some(1)
        case SqlCharVarying(lenOpt) => lenOpt orElse Some(maxVarCharLen)
        case SqlDate => Some(10)
        case SqlDecimal(precOpt, _) => precOpt orElse Some(65)
        case SqlFloat(precOpt) => precOpt orElse Some(15)
        case SqlInteger => Some(9)
        case SqlReal => Some(6)
        case SqlSmallInt => Some(4)
        case SqlTime => Some(10)
        case SqlTimestamp => Some(20)
        case SqlBool | SqlNullType | SqlText | SqlBlob | SqlClob => None
        case _ => throw new IllegalArgumentException(
            "Type (" + sqlType.repr + ") not supported"
        )
    }

    /** Get the scale of a SQL type
      *
      * @param sqlType SQL type object
      * @return Some[Int] object containing scale of the type, if defined.
      *         None if scale is not defined for this type
      */
    def scaleOpt(sqlType: SqlType): Option[Int] = sqlType match {
        case SqlOption(t) => scaleOpt(t)
        case SqlBool | SqlCharFixed(_) | SqlCharVarying(_) |
             SqlDate | SqlNullType | SqlText |
             SqlTime | SqlTimestamp | SqlBlob | SqlClob => None
        case SqlDecimal(_, sOpt@Some(_)) => sOpt
        case _ => precisionOpt(sqlType)
    }

    /** Get the octet length of a SQL type
      *
      * @param sqlType SQL type object
      * @return Some[Int] object containing octet length of the type,
      *         if defined. None if octet length is not defined for this type
      */
    def charOctetLength(sqlType: SqlType): Option[Int] = sqlType match {
        case SqlOption(t) => charOctetLength(t)
        case SqlDecimal(precOpt, scaleOpt) => scaleOpt orElse precOpt
        case SqlFloat(precOpt) => precOpt
        case SqlReal => Some(6)
        case SqlBigInt | SqlBool | SqlInteger | SqlNullType |
             SqlSmallInt | SqlText | SqlCharFixed(_) | SqlCharVarying(_) |
             SqlDate | SqlTime | SqlTimestamp | SqlBlob | SqlClob => None
        case _ => throw new IllegalArgumentException(
            "Type (" + sqlType.repr + ") not supported"
        )
    }

    /** Get the display size of a SQL type
      *
      * @param sqlType SQL type object
      * @return The display size of the type
      */
    def displaySize(sqlType: SqlType): Int = sqlType match {
        case SqlOption(t) => displaySize(t)
        case SqlBigInt => 8
        case SqlBool => 5
        case SqlCharFixed(lenOpt) => lenOpt getOrElse 1
        case SqlCharVarying(lenOpt) => lenOpt getOrElse 10
        case SqlDate => 10
        case SqlDecimal(precOpt, _) => precOpt getOrElse 8
        case SqlFloat(precOpt) => precOpt getOrElse 8
        case SqlInteger => 8
        case SqlNullType => 4
        case SqlReal => 8
        case SqlSmallInt => 8
        case SqlText => 10
        case SqlTime => 10
        case SqlTimestamp => 20
        case SqlBlob | SqlClob => 8
        case _ => throw new IllegalArgumentException(
            "Type (" + sqlType.repr + ") not supported"
        )
    }
}
