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

package com.scleradb.sql.mapper.default

import com.scleradb.util.tools.Counter

import com.scleradb.dbms.location.Location
import com.scleradb.objects._
import com.scleradb.external.expr._

import com.scleradb.sql.objects._
import com.scleradb.sql.datatypes._
import com.scleradb.sql.types._
import com.scleradb.sql.expr._
import com.scleradb.sql.statements._
import com.scleradb.sql.mapper._
import com.scleradb.sql.mapper.target._

private[scleradb]
class PostgreSQLMapper(locOpt: Option[Location]) extends SqlMapper {
    private def annotTableName(name: String): String =
        locOpt.map { loc => loc.annotTableName(name) } getOrElse name

    override def queryString(
        query: SqlRelQueryStatement
    ): String = targetQueryString(SqlTranslator.translateQuery(query))

    private def targetQueryString(
        targetQuery: TargetSqlQuery
    ): String = targetQuery match {
        case TargetSqlSelect(distinct, select, from, where,
                             group, having, order, limit, offset, _) =>
            val distinctClause: Option[String] = distinct.map {
                case Nil => "DISTINCT"
                case exprs =>
                    "DISTINCT ON (" +
                    exprs.map(expr => exprString(expr)).mkString(", ") + ")"
            }

            val selectClause: Option[String] =
                Some(select.map(expr => targetExprString(expr)).mkString(", "))

            val fromClause: Option[String] = Some("FROM " + fromString(from))

            val whereClause: Option[String] =
                where.map { predExpr => "WHERE " + exprString(predExpr) }

            val groupClause: Option[String] = group match {
                case Nil => None
                case exprs =>
                    Some("GROUP BY " +
                         exprs.map(expr => exprString(expr)).mkString(", "))
            }

            val havingClause: Option[String] =
                having.map { predExpr => "HAVING " + exprString(predExpr) }

            val orderClause: Option[String] = order match {
                case Nil => None
                case exprs =>
                    Some("ORDER BY " +
                         exprs.map(expr => sortExprString(expr)).mkString(", "))
            }

            val limitClause: Option[String] = limit.map { n => "LIMIT " + n }

            val offsetClause: Option[String] = offset.map { n => "OFFSET " + n }

            val clauses: List[Option[String]] =
                List(Some("SELECT"), distinctClause, selectClause,
                     fromClause, whereClause,
                     groupClause, havingClause,
                     orderClause, limitClause, offsetClause)

            clauses.flatten.mkString(" ")

        case TargetSqlCompound(compoundType, lhs, rhs) =>
            val compoundTypeStr: String = compoundType match {
                case Union => "UNION"
                case Intersect => "INTERSECT"
                case Except => "EXCEPT"
            }

            "(" + targetQueryString(lhs) + ") " + compoundTypeStr +
            " ALL (" + targetQueryString(rhs) + ")"
    }

    private def fromString(from: TargetSqlFrom): String = from match {
        case TargetSqlTableRef(name, Nil) => annotTableName(name)

        case TargetSqlTableRef(name, cols) =>
            annotTableName(name) +
            "(" + cols.map(col => exprString(col)).mkString(", ") + ")"

        case TargetSqlValues(name, cols, rows) if( !rows.isEmpty ) =>
            val extCols: List[ColRef] =
                cols:::rows.head.scalars.drop(cols.size).map { _ =>
                    ColRef(Counter.nextSymbol("V"))
                }

            "(VALUES " +
                rows.map(row => "(" + exprString(row) + ")").mkString(", ") +
            ")" + " AS " + name + "(" +
                extCols.map(col => exprString(col)).mkString(", ") +
            ")"

        case TargetSqlJoin(joinType, joinPred, lhsInput, rhsInput) =>
            val joinTypeStr: String = joinType match {
                case Inner => "INNER JOIN"
                case FullOuter => "FULL OUTER JOIN"
                case LeftOuter => "LEFT OUTER JOIN"
                case RightOuter => "RIGHT OUTER JOIN"
            }

            val lhsStr: String = fromString(lhsInput)
            val rhsStr: String = fromString(rhsInput)
            joinPred match {
                case JoinOn(predExpr) =>
                    lhsStr + " " + joinTypeStr + " " + rhsStr + " ON (" +
                        exprString(predExpr) +
                    ")"
                case JoinUsing(cols) =>
                    lhsStr + " " + joinTypeStr + " " + rhsStr + " USING (" +
                        cols.map(col => exprString(col)).mkString(", ") +
                    ")"
                case JoinNatural =>
                    lhsStr + " NATURAL " + joinTypeStr + " " + rhsStr
            }

        case TargetSqlNested(name, query) =>
            "(" + targetQueryString(query) + ") AS " + name

        case _ =>
            throw new RuntimeException("Cannot map (PG): " + from)
    }

    override def updateString(
        stmt: SqlUpdateStatement
    ): List[String] = stmt match {
        case SqlCreateDbSchema(dbSchema) =>
            List("CREATE SCHEMA IF NOT EXISTS " + dbSchema)

        case SqlCreateDbObject(obj, Persistent) =>
            List("CREATE " + objectString(obj))

        case SqlCreateDbObject(obj, Temporary) =>
            List("CREATE TEMPORARY " + objectString(obj))

        case SqlDropExplicit(st: SchemaTable, _) =>
            val tableTypeStr: String = st.obj.baseType match {
                case Table.BaseTable => "TABLE"
                case Table.BaseView => "VIEW"
            }

            List(
                "DROP " + tableTypeStr + " IF EXISTS " +
                annotTableName(st.obj.name)
            )

        case SqlCreateIndex(indexName, relationId, indexColRefs, pred) =>
            List(
                "CREATE INDEX " + indexName +
                " ON " + relationId.name + "(" +
                    indexColRefs.map(col => exprString(col)).mkString(", ") +
                ") WHERE " + exprString(pred)
            )

        case SqlDropIndex(indexName) =>
            List("DROP INDEX " + indexName)

        case SqlInsertValueRows(TableId(_, name), targetCols, rows) =>
            List(
                "INSERT INTO " +
                fromString(
                    TargetSqlTableRef(name, targetCols)
                ) + " VALUES " +
                rows.map(row => "(" + exprString(row) + ")").mkString(", ")
            )

        case SqlInsertQueryResult(tableId, targetCols, Values(_, rows)) =>
            updateString(SqlInsertValueRows(tableId, targetCols, rows))

        case SqlInsertQueryResult(TableId(_, name), targetCols, relExpr) =>
            List(
                "INSERT INTO " +
                fromString(
                    TargetSqlTableRef(name, targetCols)
                ) + " " + queryString(SqlRelQueryStatement(relExpr))
            )

        case SqlUpdate(tableId, cvs, pred) =>
            val setExprs: List[ScalExpr] =
                cvs.map { case (c, v) => ScalOpExpr(Equals, List(c, v)) }
            List(
                "UPDATE " + annotTableName(tableId.name) +
                " SET " + setExprs.map(e => exprString(e)).mkString(", ") +
                " WHERE " + exprString(pred)
            )

        case SqlDelete(tableId, pred) =>
            List(
                "DELETE FROM " + annotTableName(tableId.name) +
                " WHERE " + exprString(pred)
            )

        case SqlUpdateBatch(stmts) =>
            stmts.flatMap { stmt => updateString(stmt) }

        case _ =>
            throw new RuntimeException("Cannot map (PG): " + stmt)
    }

    private def objectString(obj: SqlDbObject): String = obj match {
        case SqlObjectAsExpr(name, relExpr, DbMaterialized(_)) =>
            tableString(name, relExpr)

        case SqlObjectAsExpr(name, relExpr, DbVirtual) =>
            viewString(name, relExpr)

        case SqlTable(table, _, None) =>
            tableString(table)

        case _ =>
            throw new RuntimeException("Cannot map (PG): " + obj)
    }

    def tableString(name: String, relExpr: RelExpr): String =
        "TABLE " + annotTableName(name) +
        " AS " + queryString(SqlRelQueryStatement(relExpr))

    def viewString(name: String, relExpr: RelExpr): String =
        "VIEW " + annotTableName(name) +
        " AS " + queryString(SqlRelQueryStatement(relExpr))

    def tableString(table: Table): String = {
        val colStrs: List[String] = table.columns.map {
            case Column(colName, colType, None) =>
                val colTypeStr: String = colType match {
                    case SqlOption(t) => sqlTypeString(t)
                    case t => sqlTypeString(t) + " NOT NULL"
                }

                colName + " " + colTypeStr
            case _ =>
                throw new RuntimeException("Column families not handled")
        }

        val keyStrs: List[String] = table.keyOpt.toList.map {
            case PrimaryKey(cols) =>
                "PRIMARY KEY (" +
                    cols.map(col => exprString(col)).mkString(", ") +
                ")"
        }

        val refStrs: List[String] = locOpt match {
            case Some(loc) => // for PostgreSQL
                table.foreignKeys.filter { fk =>
                    fk.refTableId(loc.schema).locationId == loc.id
                } map { case ForeignKey(cols, _, refTableName, refCols) =>
                    "FOREIGN KEY (" +
                        cols.map(col => exprString(col)).mkString(", ") +
                    ") REFERENCES " +
                    fromString(TargetSqlTableRef(refTableName, refCols))
                }

            case None => // for Sclera user interface
                table.foreignKeys.map {
                    case ForeignKey(cols, refLocIdOpt, refTableName, refCols) =>
                        val refTableIdStr: String = refLocIdOpt match {
                            case Some(refLocId) =>
                                TableId(refLocId, refTableName).repr
                            case None => refTableName
                        }

                        "FOREIGN KEY (" +
                            cols.map(col => exprString(col)).mkString(", ") +
                        ") REFERENCES " +
                        fromString(TargetSqlTableRef(refTableIdStr, refCols))
                }
        }

        "TABLE " + annotTableName(table.name) +
            "(" + (colStrs:::keyStrs:::refStrs).mkString(", ") + ")"
    }

    def exprString(scalExpr: ScalExpr): String = scalExpr match {
        case ColRef(name) => name

        case AnnotColRef(Some(tName), cName) => tName + "." + cName

        case AnnotColRef(None, cName) => cName

        case LabeledColRef(Nil, None, cName) => cName

        case LabeledColRef(List(label), None, cName) => label + "." + cName

        case LabeledColRef(List(label), Some(index), cName) =>
            label + "[" + index + "]." + cName

        case LabeledColRef(labels, None, cName) =>
            "LABEL(" + labels.mkString(", ") + ")." + cName

        case LabeledColRef(labels, Some(index), cName) =>
            "LABEL(" + labels.mkString(", ") + ")[" + index + "]." + cName

        case IntConst(value) => value.toString

        case ShortConst(value) => value.toString

        case LongConst(value) => value.toString

        case FloatConst(value) => value.toString

        case DoubleConst(value) => value.toString

        case BoolConst(value) => value.toString

        case CharConst(value) =>
            "'" + value.replaceAll("'", "''") +
            "'::CHAR(" + (value.size max 1) + ")"

        case DateConst(value) => "'" + value.toString + "'"

        case TimestampConst(value) => "'" + value.toString + "'"

        case TimeConst(value) => "'" + value.toString + "'"

        case SqlTypedNull(t) => "NULL::" + sqlTypeString(t)

        case Pattern(pat, esc) => pat + " ESCAPE " + esc

        case Row(vs) => vs.map(s => exprString(s)).mkString(", ")

        case ScalSubQuery(relExpr) => queryString(SqlRelQueryStatement(relExpr))

        case Exists(relExpr) =>
            "EXISTS (" + queryString(SqlRelQueryStatement(relExpr)) + ")"

        case ScalOpExpr(cmpOp: ScalRelCmpOp,
                        List(lhs, ScalCmpRelExpr(qual, subQueryOrList))) =>
            val cmpQualStr: String = qual match {
                case CmpAll => "ALL"
                case CmpAny => "ANY"
            }

            "(" + exprString(lhs) + ") " +
            cmpOpString(cmpOp) + " " + cmpQualStr +
            " (" + relSubQueryString(subQueryOrList) + ")"

        case ScalOpExpr(TypeCast(t), List(input)) =>
            "(" + exprString(input) + ")::" + sqlTypeString(t)

        case ScalOpExpr(UnaryPlus, List(input)) =>
            "+(" + exprString(input) + ")"

        case ScalOpExpr(UnaryMinus, List(input)) =>
            "-(" + exprString(input) + ")"

        case ScalOpExpr(Plus, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") + (" + exprString(rhs) + ")"

        case ScalOpExpr(Minus, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") - (" + exprString(rhs) + ")"

        case ScalOpExpr(Mult, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") * (" + exprString(rhs) + ")"

        case ScalOpExpr(Div, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") / (" + exprString(rhs) + ")"

        case ScalOpExpr(Modulo, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") % (" + exprString(rhs) + ")"

        case ScalOpExpr(Exp, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") ^ (" + exprString(rhs) + ")"

        case ScalOpExpr(Not, List(input)) =>
            "NOT (" + exprString(input) + ")"

        case ScalOpExpr(Or, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") OR (" + exprString(rhs) + ")"

        case ScalOpExpr(And, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") AND (" + exprString(rhs) + ")"

        case ScalOpExpr(IsNull, List(input)) =>
            "(" + exprString(input) + ") IS NULL"

        case ScalOpExpr(IsLike(pat), List(lhs)) =>
            "(" + exprString(lhs) + ") LIKE " + exprString(pat)

        case ScalOpExpr(IsILike(pat), List(lhs)) =>
            "(" + exprString(lhs) + ") ILIKE " + exprString(pat)

        case ScalOpExpr(IsSimilarTo(pat), List(lhs)) =>
            "(" + exprString(lhs) + ") SIMILAR TO " + exprString(pat)

        case ScalOpExpr(IsDistinctFrom, List(lhs, rhs)) =>
            "(" + exprString(lhs) +
            ") IS DISTINCT FROM (" + exprString(rhs) + ")"

        case ScalOpExpr(IsBetween(qual), List(expr, lhs, rhs)) =>
            val rangeQualStr: String = qual match {
                case Symmetric => "SYMMETRIC"
                case Asymmetric => ""
            }

            "(" + exprString(expr) + ") IS BETWEEN " + rangeQualStr +
            " (" + exprString(lhs) + ") AND (" + exprString(rhs) + ")"

        case ScalOpExpr(cmpOp: ScalRelCmpOp, List(ColRef(lhs), ColRef(rhs))) =>
            lhs + " " + cmpOpString(cmpOp) + " " + rhs

        case ScalOpExpr(cmpOp: ScalRelCmpOp, List(ColRef(lhs), rhs)) =>
            lhs + " " + cmpOpString(cmpOp) + " (" + exprString(rhs) + ")"

        case ScalOpExpr(cmpOp: ScalRelCmpOp, List(lhs, ColRef(rhs))) =>
            "(" + exprString(lhs) + ") " + cmpOpString(cmpOp) + " " + rhs

        case ScalOpExpr(cmpOp: ScalRelCmpOp, List(lhs, rhs)) =>
            "(" + exprString(lhs) + ") " + cmpOpString(cmpOp) +
            " (" + exprString(rhs) + ")"

        case ScalOpExpr(AggregateFunction(name, qual), inputs) =>
            val funcQualStr: String = qual match {
                case FuncAll => ""
                case FuncDistinct => "DISTINCT "
            }

            functionAlias(name) + "(" +
                funcQualStr + inputs.map(e => exprString(e)).mkString(", ") +
            ")"

        case ScalOpExpr(ScalarFunction("CURRENT_DATE"), Nil) =>
            "CURRENT_DATE"
        case ScalOpExpr(ScalarFunction("CURRENT_TIME"), Nil) =>
            "CURRENT_TIME"
        case ScalOpExpr(ScalarFunction("CURRENT_TIMESTAMP"), Nil) =>
            "CURRENT_TIMESTAMP"

        case ScalOpExpr(ScalarFunction("DATE_PART"), List(spec, d)) =>
            spec match {
                case CharConst(evalSpec)
                if evalSpec.toUpperCase == "DAY_OF_WEEK" =>
                    "(EXTRACT(DOW FROM " + exprString(d) + ") + 1)"
                case CharConst(evalSpec)
                if evalSpec.toUpperCase == "DAY_OF_YEAR" =>
                    "EXTRACT(DOY FROM " + exprString(d) + ")"
                case CharConst(evalSpec) =>
                    "EXTRACT(" + evalSpec + " FROM " + exprString(d) + ")"
                case _ =>
                    throw new IllegalArgumentException(
                        "Cannot compute DATE_PART on " +
                        "unknown specification \"" + spec.repr + "\""
                    )
            }

        case ScalOpExpr(ScalarFunction(name), inputs) =>
            functionAlias(name) +
            "(" + inputs.map(e => exprString(e)).mkString(", ") + ")"

        case ScalOpExpr(ExternalScalarFunction(f), inputs) =>
            f.toString +
            "(" + inputs.map(e => exprString(e)).mkString(", ") + ")"

        case ScalOpExpr(LabeledFunction(name, Nil), inputs) =>
            name + "(" + inputs.map(e => exprString(e)).mkString(", ") + ")"

        case ScalOpExpr(LabeledFunction(name, List(label)), inputs) =>
            label + "." + name +
            "(" + inputs.map(e => exprString(e)).mkString(", ") + ")"

        case ScalOpExpr(LabeledFunction(name, labels), inputs) =>
            "LABEL(" + labels.mkString(", ") + ")." + name +
            "(" + inputs.map(e => exprString(e)).mkString(", ") + ")"

        case CaseExpr(argExpr, whenThen, defaultExpr) =>
            List(
                List("CASE", exprString(argExpr)),
                whenThen.flatMap {
                    case (w, t) =>
                        List("WHEN", exprString(w), "THEN", exprString(t))
                },
                List("ELSE", exprString(defaultExpr), "END")
            ).flatten.mkString(" ")

        case _ => throw new RuntimeException("Cannot map (PG): " + scalExpr)
    }

    private def cmpOpString(cmpOp: ScalRelCmpOp): String = cmpOp match {
        case Equals => "="
        case NotEquals => "<>"
        case LessThan => "<"
        case LessThanEq => "<="
        case GreaterThan => ">"
        case GreaterThanEq => ">="
    }

    def targetExprString(targetExpr: TargetExpr): String = targetExpr match {
        case StarTargetExpr(Some((tname, None)), Nil) => tname + ".*"
        case StarTargetExpr(None, Nil) => "*"
        case (t: ScalarTarget) =>
            "(" + exprString(t.expr) + ") AS " + exprString(t.alias)
        case _ => throw new RuntimeException("Cannot map (PG): " + targetExpr)
    }

    def sortExprString(sortExpr: SortExpr): String = sortExpr match {
        case SortExpr(expr, sortDir, nullsOrder) =>
            val sortDirStr: String = sortDir match {
                case SortAsc => "ASC"
                case SortDesc => "DESC"
            }

            val nullsOrderStr: String = nullsOrder match {
                case NullsFirst => "NULLS FIRST"
                case NullsLast => "NULLS LAST"
            }

            "(" + exprString(expr) + ") " + sortDirStr + " " + nullsOrderStr
    }

    private def relSubQueryString(
        subQueryOrList: RelSubQueryBase
    ): String = subQueryOrList match {
        case ScalarList(exprs) =>
            val rows: List[Row] = exprs.map {
                case (v: ScalValue) => Row(List(v))
                case _ =>
                    throw new IllegalArgumentException(
                        "Only scalar values expected in the list"
                    )
            }

            targetQueryString(
                TargetSqlSelect(
                    from = TargetSqlValues(Counter.nextSymbol("Q"), Nil, rows)
                )
            )

        case RelSubQuery(relExpr) => queryString(SqlRelQueryStatement(relExpr))
    }

    def sqlTypeString(sqlType: SqlType): String = sqlType match {
        case SqlInteger => "INTEGER"
        case SqlSmallInt => "SMALLINT"
        case SqlBigInt => "BIGINT"
        case SqlDecimal(None, None) => "DECIMAL"
        case SqlDecimal(Some(p), None) => "DECIMAL(" + p + ")"
        case SqlDecimal(Some(p), Some(s)) => "DECIMAL(" + p + ", " + s + ")"
        case SqlFloat(None) => "FLOAT"
        case SqlFloat(Some(p)) => "FLOAT(" + p + ")"
        case SqlReal => "REAL"
        case SqlBool => "BOOLEAN"
        case SqlCharFixed(None) => "CHAR"
        case SqlCharFixed(Some(l)) => "CHAR(" + l + ")"
        case SqlCharVarying(None) => "VARCHAR"
        case SqlCharVarying(Some(l)) => "VARCHAR(" + l + ")"
        case SqlText => "TEXT"
        case SqlTimestamp => "TIMESTAMP"
        case SqlTime => "TIME"
        case SqlDate => "DATE"
        case SqlOption(baseType) => sqlTypeString(baseType)
        case _ => throw new RuntimeException("Cannot map (PG): " + sqlType)
    }

    override val functionMapOpt: Option[Map[String, String]] = Some(
        Map(
            "ABS" -> "ABS",
            "ACOS" -> "ACOS",
            "ASIN" -> "ASIN",
            "ATAN" -> "ATAN",
            "ATAN2" -> "ATAN2",
            "AVG" -> "AVG",
            "BOOL_AND" -> "BOOL_AND",
            "BOOL_OR" -> "BOOL_OR",
            "CEIL" -> "CEILING",
            "CEILING" -> "CEILING",
            "CHARACTER_LENGTH" -> "LENGTH",
            "CHAR_LENGTH" -> "LENGTH",
            "COALESCE" -> "COALESCE",
            "CONCAT" -> "CONCAT",
            "CORR" -> "CORR",
            "COS" -> "COS",
            "COUNT" -> "COUNT",
            "COVAR_POP" -> "COVAR_POP",
            "COVAR_SAMP" -> "COVAR_SAMP",
            "CURRENT_DATE" -> "CURRENT_DATE",
            "CURRENT_TIME" -> "CURRENT_TIME",
            "CURRENT_TIMESTAMP" -> "CURRENT_TIMESTAMP",
            "DATE_PART" -> "DATE_PART",
            "EXP" -> "EXP",
            "FLOOR" -> "FLOOR",
            "GREATEST" -> "GREATEST",
            "LEAST" -> "LEAST",
            "LENGTH" -> "LENGTH",
            "LOG" -> "LOG",
            "LOG10" -> "LOG10",
            "LOWER" -> "LOWER",
            "MAX" -> "MAX",
            "MIN" -> "MIN",
            "MOD" -> "MOD",
            "NULLIF" -> "NULLIF",
            "POWER" -> "POWER",
            "RANDOM" -> "RANDOM",
            "REGR_AVGX" -> "REGR_AVGX",
            "REGR_AVGY" -> "REGR_AVGY",
            "REGR_COUNT" -> "REGR_COUNT",
            "REGR_INTERCEPT" -> "REGR_INTERCEPT",
            "REGR_R2" -> "REGR_R2",
            "REGR_SLOPE" -> "REGR_SLOPE",
            "REGR_SXX" -> "REGR_SXX",
            "REGR_SXY" -> "REGR_SXY",
            "REGR_SYY" -> "REGR_SYY",
            "REPLACE" -> "REPLACE",
            "ROUND" -> "ROUND",
            "SIGN" -> "SIGN",
            "SIN" -> "SIN",
            "SQRT" -> "SQRT",
            "STDDEV" -> "STDDEV",
            "STDDEV" -> "STDDEV_SAMP",
            "STDDEV_POP" -> "STDDEV_POP",
            "STDDEV_SAMP" -> "STDDEV_SAMP",
            "STRPOS" -> "INSTR",
            "SUBSTRING" -> "SUBSTRING",
            "SUM" -> "SUM",
            "TAN" -> "TAN",
            "TRIM" -> "TRIM",
            "TRUNC" -> "TRUNC",
            "UPPER" -> "UPPER",
            "VARIANCE" -> "VARIANCE",
            "VARIANCE" -> "VAR_SAMP",
            "VAR_POP" -> "VAR_POP",
            "VAR_SAMP" -> "VAR_SAMP"
        )
    )
}

// SQL mapper for Sclera user interface
private[scleradb]
object PostgreSQLMapper extends PostgreSQLMapper(None)
