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

package com.scleradb.api

import java.sql.{Time, Timestamp, Date, Blob, Clob}

import scala.util.parsing.input.CharArrayReader

import com.scleradb.sql.parser.SqlQueryParser
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableRow

import com.scleradb.sql.objects._
import com.scleradb.sql.expr._
import com.scleradb.sql.types._

// representation of a sql/scala datatype in the database
sealed abstract class DbStruct[T] {
    def fromDb(rs: TableRow): T // rs -> value

    // does this dbStruct include the given column dbstruct?
    def includes[U](dbs: DbStruct[U]): Boolean =
        (dbs.columns diff columns).isEmpty

    // number of underlying columns
    def size: Int

    // underlying columns
    def columns: List[Column]

    // underlying columns' names
    def columnRefs: List[ColRef]

    // scalar value of each underlying column
    def scalars(value: T): List[ScalColValue]

    // predicate for comparision with a value
    def comparePredicate(op: ScalRelCmpOp, value: T): ScalExpr = {
        val exprs: List[ScalExpr] = columnRefs.zip(scalars(value)) map {
            case (colRef, colVal) => ScalOpExpr(op, List(colRef, colVal))
        }

        exprs.tail.foldLeft (exprs.head) {
            case (expr, next) => ScalOpExpr(And, List(expr, next))
        }
    }

    def clone(rename: String => String): DbStruct[T]
}

// wrapper struct
abstract class Wrapper[A, B] {
    def wrap(baseVal: A): B
    def unwrap(wrappedVal: B): A
}

case class DbWrapperStruct[A, B](
    baseDbs: DbStruct[A],
    wrapper: Wrapper[A, B]
) extends DbStruct[B] {
    override def fromDb(rs: TableRow): B = wrapper.wrap(baseDbs.fromDb(rs))

    override def size: Int = baseDbs.size

    override def columns: List[Column] = baseDbs.columns

    override def columnRefs: List[ColRef] = baseDbs.columnRefs

    override def scalars(value: B): List[ScalColValue] =
        baseDbs.scalars(wrapper.unwrap(value))

    override def clone(rename: String => String): DbWrapperStruct[A, B] =
        DbWrapperStruct(baseDbs.clone(rename), wrapper)

    def unwrapped: DbStruct[A] = baseDbs
}

case class DbPairStruct[A, B](
    left: DbStruct[A], right: DbStruct[B]
) extends DbStruct[(A, B)] {
    override def fromDb(rs: TableRow): (A, B) =
        (left.fromDb(rs), right.fromDb(rs))

    override def size: Int = left.size + right.size

    override def columns: List[Column] = left.columns:::right.columns

    override def columnRefs: List[ColRef] = left.columnRefs:::right.columnRefs

    override def scalars(value: (A, B)): List[ScalColValue] = value match {
        case (lv, rv) => left.scalars(lv) ::: right.scalars(rv)
    }

    override def clone(rename: String => String): DbPairStruct[A, B] =
        DbPairStruct(left.clone(rename), right.clone(rename))
}
    
sealed abstract class DbColStruct[T] extends DbStruct[T] {
    def colName: String
    def sqlType: SqlTypeRoot[T]

    def fromDb(rs: TableRow): T

    override def size: Int = 1

    override def columns: List[Column] = List(column)

    def column: Column

    override def columnRefs: List[ColRef] = List(columnRef)
    def columnRef: ColRef

    override def scalars(value: T): List[ScalColValue] = List(scalar(value))
    def scalar(value: T): ScalColValue

    override def clone(rename: String => String): DbColStruct[T] =
        sqlType.dbStruct(rename(colName))
}

sealed abstract class DbBaseTypeColStruct[T] extends DbColStruct[T] {
    def sqlType: SqlBaseType[T]
    override def column: Column = Column(colName, sqlType)
    override def columnRef: ColRef = ColRef(colName)

    def fromDbOpt(rs: TableRow): Option[T]
    override def fromDb(rs: TableRow): T = fromDbOpt(rs).getOrElse {
        throw new RuntimeException("Found NULL in column (" + colName + ")")
    }

    def scalar(value: T): ScalValueBase
}

// database representation of Base types
case class IntStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Int] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Int] = SqlInteger

    override def fromDbOpt(rs: TableRow): Option[Int] =
        rs.getIntOpt(colName)

    override def scalar(value: Int): ScalValueBase = IntConst(value)
}

case class ShortStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Short] {
    override def sqlType: SqlBaseType[Short] = SqlSmallInt

    override def fromDbOpt(rs: TableRow): Option[Short] =
        rs.getShortOpt(colName)

    override def scalar(value: Short): ScalValueBase = ShortConst(value)
}

case class LongStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Long] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Long] = SqlBigInt

    override def fromDbOpt(rs: TableRow): Option[Long] =
        rs.getLongOpt(colName)

    override def scalar(value: Long): ScalValueBase = LongConst(value)
}

case class FloatStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Float] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Float] = SqlReal

    override def fromDbOpt(rs: TableRow): Option[Float] =
        rs.getFloatOpt(colName)

    override def scalar(value: Float): ScalValueBase = FloatConst(value)
}

case class DoubleStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Double] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Double] = SqlFloat(None)

    override def fromDbOpt(rs: TableRow): Option[Double] =
        rs.getDoubleOpt(colName)

    override def scalar(value: Double): ScalValueBase = DoubleConst(value)
}

case class BoolStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Boolean] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Boolean] = SqlBool

    override def fromDbOpt(rs: TableRow): Option[Boolean] =
        rs.getBooleanOpt(colName)

    override def scalar(value: Boolean): ScalValueBase = BoolConst(value)
}

case class StringStruct(
    override val colName: String
) extends DbBaseTypeColStruct[String] {
    assert(colName != null)

    override def sqlType: SqlBaseType[String] = SqlText

    override def fromDbOpt(rs: TableRow): Option[String] =
        rs.getStringOpt(colName)

    override def scalar(value: String): ScalValueBase = CharConst(value)
}

case class DateStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Date] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Date] = SqlDate

    override def fromDbOpt(rs: TableRow): Option[Date] =
        rs.getDateOpt(colName)

    override def scalar(value: Date): ScalValueBase = DateConst(value)
}

case class TimestampStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Timestamp] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Timestamp] = SqlTimestamp

    override def fromDbOpt(rs: TableRow): Option[Timestamp] =
        rs.getTimestampOpt(colName)

    override def scalar(value: Timestamp): ScalValueBase = TimestampConst(value)
}

case class TimeStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Time] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Time] = SqlTime

    override def fromDbOpt(rs: TableRow): Option[Time] =
        rs.getTimeOpt(colName)

    override def scalar(value: Time): ScalValueBase = TimeConst(value)
}

case class BlobStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Blob] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Blob] = SqlBlob

    override def fromDbOpt(rs: TableRow): Option[Blob] =
        rs.getBlobOpt(colName)

    override def scalar(value: Blob): ScalValueBase = BlobConst(value)
}

case class ClobStruct(
    override val colName: String
) extends DbBaseTypeColStruct[Clob] {
    assert(colName != null)

    override def sqlType: SqlBaseType[Clob] = SqlClob

    override def fromDbOpt(rs: TableRow): Option[Clob] =
        rs.getClobOpt(colName)

    override def scalar(value: Clob): ScalValueBase = ClobConst(value)
}

// database representation of option
case class DbOptionStruct[A](
    baseDbs: DbBaseTypeColStruct[A]
) extends DbColStruct[Option[A]] {
    override val colName: String = baseDbs.colName
    assert(colName != null)

    override def sqlType: SqlTypeRoot[Option[A]] = SqlOption(baseDbs.sqlType)

    override def fromDb(rs: TableRow): Option[A] = baseDbs.fromDbOpt(rs)

    override def scalar(vOpt: Option[A]): ScalColValue = vOpt match {
        case Some(v) => baseDbs.scalar(v)
        case None => SqlNull(baseDbs.sqlType)
    }

    override def column: Column =
        Column(baseDbs.colName, SqlOption(baseDbs.sqlType))
    override def columnRef: ColRef = baseDbs.columnRef
}

case object NullStruct extends DbBaseTypeColStruct[Null] {
    override def colName: String = null
    override def sqlType: SqlBaseType[Null] = SqlNullType

    override def column: Column = null
    override def columnRef: ColRef = null

    override def fromDbOpt(rs: TableRow): Option[Null] = Some(null)

    override def size: Int = 0
    override def columns: List[Column] = Nil
    override def columnRefs: List[ColRef] = Nil
    override def scalars(value: Null): List[ScalColValue] = Nil

    override def scalar(value: Null): ScalValueBase = null
    override def clone(rename: String => String): DbBaseTypeColStruct[Null] =
        NullStruct
}
