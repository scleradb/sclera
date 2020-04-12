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

package com.scleradb.sql.parser

import java.sql.Types

import com.scleradb.pgcatalog.PgCatalog

import com.scleradb.dbms.location.LocationId

import com.scleradb.util.automata.datatypes.Label
import com.scleradb.util.tools.Counter

import com.scleradb.sql.types._
import com.scleradb.sql.objects._
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.expr._
import com.scleradb.sql.statements.{SqlStatement, SqlRelQueryStatement}
import com.scleradb.sql.exec.ScalCastEvaluator

import com.scleradb.external.objects.{ExternalSource, ExternalFunction}
import com.scleradb.external.expr.{ExternalSourceExpr, ExternalScalarFunction}

import com.scleradb.external.objects.SequenceSource

import com.scleradb.analytics.nlp.expr.NlpRelOp

import com.scleradb.analytics.ml.objects.MLObjectId
import com.scleradb.analytics.ml.imputer.datatypes.ImputeSpec
import com.scleradb.analytics.ml.imputer.expr.Impute
import com.scleradb.analytics.ml.classifier.expr.Classify
import com.scleradb.analytics.ml.clusterer.expr.Cluster

import com.scleradb.analytics.sequence.labeler._
import com.scleradb.analytics.sequence.matcher._
import com.scleradb.analytics.sequence.matcher.aggregate._
import com.scleradb.analytics.sequence.matcher.expr.{Match, LabeledMatch, Pivot}

import com.scleradb.analytics.infertypes.expr.InferTypes

// SQL query parser
trait SqlQueryParser extends SqlParser {
    override def sqlStatement: Parser[SqlStatement] = sqlQueryStatement

    // postgresql catalog
    private lazy val pgCatalog: PgCatalog = PgCatalog(schema)

    // delimiters and reserved keywords
    lexical.delimiters ++= Seq(
        "!", "~", "^", "<", "=", ">", "-", ",",
        "::", "/", ".", "(", ")", "*", "%", "+", "[", "]", "||"
    )

    lexical.reserved ++= Seq(
        "ALL", "ALIGN", "AND", "ANY", "ARG", "AS", "ASC",
        "BETWEEN", "BIGINT", "BIT", "BOOL", "BOOLEAN", "BPCHAR", "BY",
        "CASE", "CAST", "CHAR", "CLASSIFIED", "CLUSTERED",
        "COLUMN", "CONNECTED", "CROSS",
        "DATE", "DECIMAL", "DESC", "DISTINCT",
        "ELSE", "END", "ESCAPE", "EXCEPT", "EXISTS", "EXTERNAL",
        "FALSE", "FETCH", "FIRST", "FLAG", "FLOAT", "FOR", "FROM", "FULL",
        "GROUP", "HAVING", "ILIKE", "IMPUTED", "IN", "INNER", "INT", "INTEGER",
        "INTERSECT", "INTERVAL", "INTO",
        "IS", "ISNULL",
        "JOIN", "LABEL", "LAST", "LEFT", "LIKE", "LIMIT",
        "MARGIN", "MATCH",
        "NATURAL", "NEXT", "NOT", "NOTNULL", "NULL", "NULLS", "NUMERIC",
        "OFFSET", "ON", "ONLY", "OR", "ORDER", "ORDERED", "OUTER", "OVER",
        "PARTITION", "PG_CATALOG", "PIVOT",
        "REAL", "RIGHT", "ROW", "ROWS",
        "SELECT", "SEQUENCE",
        "SIMILAR", "SMALLINT", "SOME", "SPLIT", "SYMMETRIC",
        "TABLE", "TEXT", "THEN", "TIME", "TIMESTAMP", "TO", "TRUE", "TYPEINFER",
        "UNION", "UNKNOWN", "UNPIVOT", "USING",
        "VALUES", "VARBIT", "VARCHAR", "VARCHAR2", "VARYING", "VIEW",
        "WHEN", "WHERE", "WITH", "WITHOUT", "ZONE"
    )

    // SQL query parser
    def sqlQueryStatement: Parser[SqlRelQueryStatement] =
        relExpr ^^ { expr => SqlRelQueryStatement(expr) }

    def relExprPar: Parser[RelExpr] = "(" ~> relExpr <~ ")"
    override def relExpr: Parser[RelExpr] =
        compoundTableExpr ~ opt(orderClause) ~ opt(limitOffsetClause) ^^ {
            case expr~orderOpt~limitOffsetFuncOpt =>
                RelExpr.orderLimitExpr(expr, orderOpt, limitOffsetFuncOpt)
        }

    def compoundTableExpr: Parser[RelExpr] =
        compoundTableExcept ~
        rep("UNION" ~> compoundQual ~ compoundTableExcept) ^^ {
            case expr~exprs => exprs.foldLeft (expr) {
                case (accExpr, qual~nxt) =>
                    qual(RelOpExpr(Compound(Union), List(accExpr, nxt)))
            }
        }

    def compoundTableExcept: Parser[RelExpr] =
        compoundTableIntersect ~
        rep("EXCEPT" ~> compoundQual ~ compoundTableIntersect) ^^ {
            case expr~exprs => exprs.foldLeft (expr) {
                case (accExpr, qual~nxt) =>
                    qual(RelOpExpr(Compound(Except), List(accExpr, nxt)))
            }
        }

    def compoundTableIntersect: Parser[RelExpr] =
        compoundTableElem ~
        rep("INTERSECT" ~> compoundQual ~ compoundTableElem) ^^ {
            case expr~exprs => exprs.foldLeft (expr) {
                case (accExpr, qual~nxt) =>
                    qual(RelOpExpr(Compound(Intersect), List(accExpr, nxt)))
            }
        }

    def compoundQual: Parser[RelExpr => RelExpr] =
        opt("ALL" | "DISTINCT") ^^ {
            case Some("ALL") =>
                relExpr => relExpr // identity
            case Some("DISTINCT") | None => // default = distinct
                relExpr => RelOpExpr(Distinct, List(relExpr))
            case Some(x) => throw new RuntimeException("Unexpected input: " + x)
        }

    def compoundTableElem: Parser[RelExpr] =
        joinTableExpr ~ opt(whereClause) ^^ {
            case join~Some(select) => RelOpExpr(select, List(join))
            case join~None => join
        } |
        simpleSelect

    def joinTableExpr: Parser[RelExpr] =
        joinTableElem ~ rep(joinTablePostExpr) ^^ {
            case relExpr~fList => fList.foldLeft (relExpr) ((x, f) => f(x))
        }

    def joinTablePostExpr: Parser[RelExpr => RelExpr] =
        (joinSpecial <~ "JOIN") ~ joinTableElem ^^ {
            case join~rhs =>
                (lhs: RelExpr) => RelOpExpr(join, List(lhs, rhs))
        } |
        (joinType <~ "JOIN") ~ joinTableElem ~ joinPred ^^ {
            case joinType~rhs~joinPred =>
                (lhs: RelExpr) =>
                    RelOpExpr(Join(joinType, joinPred), List(lhs, rhs))
        } |
        "ALIGN" ~> joinTableElem ~
        opt("ON" ~> scalArithExpr ~ opt("MARGIN" ~> rawIntConst)) ^^ {
            case rhs ~ Some(distExpr ~ marginOpt) =>
                (lhs: RelExpr) =>
                    RelOpExpr(Align(distExpr, marginOpt), List(lhs, rhs))
            case rhs ~ None =>
                (lhs: RelExpr) =>
                    RelOpExpr(Align(IntConst(0), None), List(lhs, rhs))
        }


    def joinSpecial: Parser[Join] =
        "CROSS" ^^^ { Join(Inner, JoinOn(BoolConst(true))) } |
        ("NATURAL" ~> joinType) ^^ { joinType => Join(joinType, JoinNatural) }

    def joinType: Parser[JoinType] =
        opt("INNER" | (("FULL" | "LEFT" | "RIGHT") <~ opt("OUTER"))) ^^ {
            case Some("FULL") => FullOuter
            case Some("LEFT") => LeftOuter
            case Some("RIGHT") => RightOuter
            case Some("INNER") | None => Inner // default = Inner
            case Some(x) => throw new RuntimeException("Unexpected input: " + x)
        }

    def joinPred: Parser[JoinPred] =
        ("USING" ~> colRefOrListPar) ^^ { cols => JoinUsing(cols) } |
        opt("ON" ~> scalExpr) ^^ {
            case Some(expr) => JoinOn(expr)
            case None => JoinOn(BoolConst(true))
        }

    def joinTableElem: Parser[RelExpr] =
        relExprBaseElem ~ rep(baseRelOp) ^^ {
            case expr~ops => ops.foldLeft (expr) { case (prev, op) => op(prev) }
        }

    def baseRelOp: Parser[RelExpr => RelExpr] =
        extRelOp ^^ {
            op => (expr: RelExpr) => RelOpExpr(op, List(expr))
        } |
        "TYPEINFER" ~> opt("(" ~>
            repsep(targetWildCardNoIndex | targetExprNoAlias, ",") ~
            opt("NULLS" ~> "(" ~> rep1sep(rawCharConst, ",") <~ ")") ~
            opt(limitClauseOpt)
        <~ ")") ^^ {
            case Some(targets~nullsOpt~lookAheadOpt) =>
                val updTargets: List[TargetExpr] =
                    if( targets.isEmpty ) List(StarTargetExpr(None, Nil))
                    else targets
                val nulls: List[String] = nullsOpt getOrElse Nil
                (expr: RelExpr) => RelOpExpr(
                    InferTypes(updTargets, nulls, lookAheadOpt.flatten, expr),
                    List(expr)
                )

            case None =>
                val targets: List[TargetExpr] = List(StarTargetExpr(None, Nil))
                (expr: RelExpr) => RelOpExpr(
                    InferTypes(targets, Nil, None, expr),
                    List(expr)
                )
        } |
        tableAliasPartition ~ opt(partitionClause) ~ opt(partitionedOpExpr) ^^ {
            case tableAlias~partnColsOpt~opExprOpt => (expr: RelExpr) =>
                val partnCols: List[ColRef] = partnColsOpt getOrElse Nil
                val aliasedExpr: RelExpr =
                    RelOpExpr(tableAlias(partnCols), List(expr))
                opExprOpt match {
                    case Some(opExpr) => opExpr(aliasedExpr)(partnCols)
                    case None => aliasedExpr
                }
        } |
        partitionClause ~ opt(partitionedOpExpr) ^^ {
            case partnCols~Some(opExpr) =>
                (expr: RelExpr) => opExpr(expr)(partnCols)
            case partnCols~None =>
                (expr: RelExpr) =>
                    val aliasName: String = expr match {
                        case RelOpExpr(TableAlias(name, _, _), _, _) => name
                        case (baseExpr: RelBaseExpr) => baseExpr.name
                        case _ => Counter.nextSymbol("P")
                    }

                    val alias: TableAlias =
                        TableAlias(aliasName, Nil, partnCols)
                    RelOpExpr(alias, List(expr))
        } |
        partitionedOpExpr ^^ { opExpr => (expr: RelExpr) => opExpr(expr)(Nil) }

    def partitionedOpExpr: Parser[RelExpr => List[ColRef] => RelExpr] =
        (matchOp | argMatchOp | pivotOp | splitOp) ^^ { op =>
            (inp: RelExpr) => (partnCols: List[ColRef]) =>
                RelOpExpr(op(partnCols), List(inp))
        }

    def relExprBaseElem: Parser[RelExpr] =
        extensionRelExpr | tableDeclClause | valuesElem |
        relExprPar ^^ {
            case (expr: RelBaseExpr) => expr
            case expr@RelOpExpr(_: TableAlias, _, _) => expr
            case expr@RelOpExpr(_: Join, _, _) => expr
            case expr => // add a table alias
                RelOpExpr(TableAlias(Counter.nextSymbol("T")), List(expr))
        }

    def tableAlias: Parser[TableAlias] =
        tableAliasPartition ^^ { alias => alias(Nil) }

    def tableAliasPartition: Parser[List[ColRef] => TableAlias] =
        opt("AS") ~> ident ~ opt("(" ~> colRefList <~ ")") ^^ {
            case tableName~colsOpt =>
                (partnCols: List[ColRef]) =>
                    TableAlias(tableName, colsOpt getOrElse Nil, partnCols)
        }

    def argMatchOp: Parser[List[ColRef] => LabeledMatch] =
        "ARG" ~> matchArgSpecOrParList ~ opt(opt("OVER") ~> matchOp) ^^ {
            case specs~Some(matchOp) =>
                val arg: SeqArgOptsSpec = SeqArgOptsSpec(specs)

                (partnCols: List[ColRef]) => {
                    val op: Match = matchOp(partnCols)
                    val matcher: RowSequenceMatcher =
                        RowSequenceMatcher(op.anchoredNfa, arg, partnCols)
                    LabeledMatch(op.labeler, matcher)
                }
            case specs~None =>
                val arg: SeqArgOptsSpec = SeqArgOptsSpec(specs)

                (partnCols: List[ColRef]) => {
                    val labeler: ConstRowLabeler = ConstRowLabeler(Label("X"))
                    val matcher: LabelSequenceMatcher =
                        LabelSequenceMatcher(arg, partnCols, false, false)
                    LabeledMatch(labeler, matcher)
                }
        }

    def matchOp: Parser[List[ColRef] => Match] =
        "MATCH" ~> regex ~ opt("ON" ~> labelerDef) ^^ {
            case regexStr~labelerOpt =>
                (partnCols: List[ColRef]) =>
                    Match(regexStr, labelerOpt, partnCols)
        }

    def labelerDef: Parser[RowLabeler] =
        colRef ~ opt(labelWhenClauseList ~ opt(labelElse)) ~ opt(labelAll) ^^ {
            case arg~Some(whenThenList~elseOpt)~allOpt =>
                ColumnRowLabeler(
                    arg, whenThenList, elseOpt, allOpt getOrElse Nil
                )
            case arg~None~allOpt =>
                ColumnRowLabeler(arg, Nil, None, allOpt getOrElse Nil)
        } |
        rep1("LABEL" ~> labelOrListPar ~ ("WHEN" ~> scalExpr)) ^^ {
            predLabels => PredRowLabeler(
                predLabels.flatMap { case labels~pred =>
                    labels.map { label => (pred, Label(label)) }
                }
            )
        }

    def labelWhenClauseList: Parser[List[(List[ScalColValue], List[Label])]] =
        rep1(labelWhenClause)

    def labelWhenClause: Parser[(List[ScalColValue], List[Label])] =
        "WHEN" ~> scalBaseExprOrListPar ~ ("THEN" ~> labelOrListPar) ^^ {
            case whens~thens => (whens, thens.map { label => Label(label) })
        }

    def labelElse: Parser[List[Label]] =
        "ELSE" ~> labelOrListPar ^^ {
            labels => labels.map { label => Label(label) }
        }

    def labelAll: Parser[List[Label]] =
        "ALL" ~> labelOrListPar ^^ {
            labels => labels.map { label => Label(label) }
        }

    def pivotOp: Parser[List[ColRef] => LabeledMatch] =
        "PIVOT" ~> matchFnName ~ opt("(" ~> funcArgList <~ ")") ~
        ("FOR" ~> colRef) ~ ("IN" ~> pivotTargetOrListPar) ^^ {
            case fname~argsOpt~pivotCol~pivotTargets =>
                (partnCols: List[ColRef]) =>
                    Pivot(
                        fname, argsOpt getOrElse Nil,
                        pivotCol, pivotTargets, partnCols
                    )
        }

    def pivotTargetOrListPar: Parser[List[(List[Label], Option[ColRef])]] =
        pivotTarget ^^ { pt => List(pt) } |
        pivotTargetListPar

    def pivotTargetListPar: Parser[List[(List[Label], Option[ColRef])]] =
        "(" ~> rep1sep(pivotTarget, ",") <~ ")"

    def pivotTarget: Parser[(List[Label], Option[ColRef])] =
        labelOrMarkedList ~ opt("AS" ~> colRef) ^^ {
            case labels~colRefOpt => (labels.map { s => Label(s) }, colRefOpt)
        }

    def splitOp: Parser[List[ColRef] => DisjointInterval] =
        "SPLIT" ~> ("(" ~> colRef) ~ ("," ~> colRef <~ ")") ~
        ("INTO" ~> "(" ~> colRef) ~ ("," ~> colRef <~ ")") ^^ {
            case inpLhsCol~inpRhsCol~outLhsCol~outRhsCol =>
                (partnCols: List[ColRef]) =>
                    DisjointInterval(
                        inpLhsCol, inpRhsCol,
                        outLhsCol, outRhsCol,
                        partnCols
                    )
        }

    def tableId: Parser[TableId] =
        ident ~ opt("." ~> ident) ^^ {
            case tableName~None => 
                TableRefSourceByName(schema, tableName).tableId
            case loc~Some(tableName) =>
                TableId(LocationId(loc), tableName)
    }

    def tableDeclClause: Parser[RelBaseExpr] =
        "TABLE" ~> tableDecl | "VIEW" ~> viewDecl | tableOrViewDecl

    def tableDecl: Parser[TableRefSource] =
        ident ~ opt("." ~> ident) ~ opt(colRefListPar) ^^ {
            case tableName~None~cRefsOpt =>
                TableRefSourceByName(schema, tableName, cRefsOpt.toList.flatten)
            case loc~Some(tableName)~cRefsOpt =>
                val tableId: TableId = TableId(LocationId(loc), tableName)
                TableRefSourceById(schema, tableId, cRefsOpt.toList.flatten)
        }

    def viewDecl: Parser[ViewRef] =
        ident ~ opt(colRefListPar) ^^ {
            case viewName~Some(cRefs) => ViewRefByName(schema, viewName, cRefs)
            case viewName~None => ViewRefByName(schema, viewName, Nil)
        }

    def tableOrViewDecl: Parser[RelBaseExpr] =
        (ident | "PG_CATALOG") ~ opt("." ~> ident) ~ opt(colRefListPar) ^^ {
            case "PG_CATALOG"~Some(tname)~None =>
                pgCatalog.table(tname)
            case (
                tname@("PG_TYPE" | "PG_CLASS" |
                       "PG_ATTRIBUTE" | "PG_ATTRDEF" | "PG_NAMESPACE")
            )~None~None =>
                pgCatalog.table(tname)
            case tname~None~csOpt =>
                val cs: List[ColRef] = csOpt getOrElse Nil
                RelExpr.relRefSource(schema, tname, cs)
            case "SCLERA"~Some(vname)~csOpt =>
                val cs: List[ColRef] = csOpt getOrElse Nil
                RelExpr.viewRef(schema, vname, cs)
            case loc~Some(tname)~csOpt =>
                val cs: List[ColRef] = csOpt getOrElse Nil
                TableRefSourceById(schema, TableId(LocationId(loc), tname), cs)
        }

    def valuesElem: Parser[RelExpr] =
        valuesClause ~ opt(tableAlias) ^^ {
            case rows~aliasOpt => RelExpr.values(schema, rows, aliasOpt)
        }

    def valuesClause: Parser[List[Row]] = "VALUES" ~> valuesRowList
    def valuesRowList: Parser[List[Row]] = rep1sep(valuesRow, ",")
    def valuesRow: Parser[Row] = scalBaseExprListPar ^^ { exprs => Row(exprs) }

    def simpleSelect: Parser[RelExpr] = "SELECT" ~> distinctExpr

    def distinctExpr: Parser[RelExpr] =
        distinctClause ~ projectExpr ^^ {
            case Some(distinct)~projExpr => RelOpExpr(distinct, List(projExpr))
            case None~projExpr => projExpr
        }

    def projectExpr: Parser[RelExpr] =
        targetExprList ~ selectExpr ~ opt(groupClause) ~ opt(havingClause) ^^ {
            case targets~inp~grpExprsOpt~havingPredOpt =>
                RelOpExpr(
                    ProjectBase(targets, inp, grpExprsOpt, havingPredOpt),
                    List(inp)
                )
        }

    def selectExpr: Parser[RelExpr] =
        baseExpr ~ opt(whereClause) ^^ {
            case baseExpr~Some(select) => RelOpExpr(select, List(baseExpr))
            case baseExpr~None => baseExpr
        }

    def baseExpr: Parser[RelExpr] =
        fromClause ^^ {
            case expr::exprs => // cross-product of the FROM clause elements
                exprs.foldLeft (expr) {
                    (lhs, rhs) => RelOpExpr(Join(Inner,
                                                 JoinOn(BoolConst(true))),
                                            List(lhs, rhs))
                }
            case Nil => // FROM clause absent
                RelExpr.singleton(schema)
        }

    def distinctClause: Parser[Option[DistinctBase]] =
        ("DISTINCT" ~> opt("ON" ~> scalExprListPar)) ^^ {
            case Some(exprs) => Some(DistinctOn(exprs, Nil))
            case None => Some(Distinct)
        } |
        opt("ALL") ^^^ { None }

    def targetExprList: Parser[List[TargetExpr]] =
        rep1sep((targetWildCard | targetExpr), ",")

    def targetWildCard: Parser[StarTargetExpr] =
        (opt(ident ~ opt("[" ~> signedRawIntConst <~ "]") <~ ".") <~ "*") ~
        opt("EXCEPT" ~> colRefOrListPar) ^^ {
            case Some(t~indexOpt)~exceptColsOpt =>
                StarTargetExpr(Some((t, indexOpt)), exceptColsOpt getOrElse Nil)
            case None~exceptColsOpt =>
                StarTargetExpr(None, exceptColsOpt getOrElse Nil)
        }

    def targetExpr: Parser[ScalarTarget] =
        scalExpr ~ opt(alias) ^^ {
            case expr~Some(alias) => ScalarTarget(expr, alias)
            case expr~None => ScalarTarget(expr, ColRef(expr.defaultAlias))
        }

    def targetWildCardNoIndex: Parser[StarTargetExpr] =
        (opt(ident <~ ".") <~ "*") ~ opt("EXCEPT" ~> colRefOrListPar) ^^ {
            case Some(t)~exceptColsOpt =>
                StarTargetExpr(Some((t, None)), exceptColsOpt getOrElse Nil)
            case None~exceptColsOpt =>
                StarTargetExpr(None, exceptColsOpt getOrElse Nil)
        }

    def targetExprNoAlias: Parser[ScalarTarget] =
        scalExpr ^^ { expr => ScalarTarget(expr, ColRef(expr.defaultAlias)) }

    def alias: Parser[ColRef] = opt("AS") ~> colRef

    def fromClause: Parser[List[RelExpr]] =
        opt("FROM" ~> rep1sep(joinTableExpr, ",")) ^^ {
            case Some(relExprList) => relExprList
            case None => Nil
        }

    def whereClause: Parser[Select] =
        "WHERE" ~> scalExpr ^^ { expr => Select(expr) }

    def groupClause: Parser[List[ScalExpr]] =
        "GROUP" ~> "BY" ~> scalExprNonEmptyList

    def havingClause: Parser[ScalExpr] = "HAVING" ~> scalExpr

    def orderClause: Parser[Order] =
        "ORDER" ~> "BY" ~> orderedScalExprList ^^ { exprs => Order(exprs) }

    def orderedByClause: Parser[List[SortExpr]] =
        "ORDERED" ~> "BY" ~> orderedScalExprOrListPar

    def orderedScalExprOrListPar: Parser[List[SortExpr]] =
        orderedScalExpr ^^ { expr => List(expr) } |
        "(" ~> orderedScalExprList <~ ")"

    def orderedScalExprList: Parser[List[SortExpr]] =
        rep1sep(orderedScalExpr, ",")

    def orderedScalExpr: Parser[SortExpr] =
        scalExpr ~ sortDir ~ opt("NULLS" ~> nullsOrder) ^^ {
            case expr~sortDir~None =>
                SortExpr(expr, sortDir, sortDir.defaultNullsOrder)
            case expr~sortDir~Some(nullsOrder) =>
                SortExpr(expr, sortDir, nullsOrder)
        }

    def sortDir: Parser[SortDir] = opt("ASC" | "DESC") ^^ {
        case Some("ASC") => SortAsc
        case Some("DESC") => SortDesc
        case None => SortAsc
        case Some(x) => throw new RuntimeException("Unexpected input: " + x)
    }

    def nullsOrder: Parser[NullsOrder] =
        "FIRST" ^^^ { NullsFirst } |
        "LAST" ^^^ { NullsLast }

    def partitionClause: Parser[List[ColRef]] =
        "PARTITION" ~> "BY" ~> colRefOrListPar

    def limitOffsetClause: Parser[List[SortExpr] => LimitOffset] =
        (limitClauseOpt ~ opt(offsetClause)) ^^ {
            case limitOpt~offsetOpt =>
                { sortExprs: List[SortExpr] =>
                    LimitOffset(limitOpt, offsetOpt getOrElse 0, sortExprs) }
        } |
        (offsetClause ~ opt(limitClauseOpt)) ^^ {
            case offset~limitOptOpt =>
                { sortExprs: List[SortExpr] =>
                    LimitOffset(limitOptOpt getOrElse None, offset, sortExprs) }
        }

    def limitClauseOpt: Parser[Option[Int]] =
        "LIMIT" ~> limitCountOpt <~ opt("ROW" | "ROWS") |
        "FETCH" ~> opt("FIRST" | "NEXT") ~> opt(limitCountOpt) <~
        opt("ROW" | "ROWS") <~ "ONLY" ^^ {
            limitOptOpt => limitOptOpt getOrElse Some(1)
        }

    def limitCountOpt: Parser[Option[Int]] =
        "ALL" ^^^ { None } |
        rawIntConst ^^ { n => Some(n) }

    def offsetClause: Parser[Int] =
        "OFFSET" ~> rawIntConst <~ opt("ROW" | "ROWS")

    def scalRelExpr: Parser[ScalExpr] =
        relExpr ^^ { relExpr => ScalSubQuery(relExpr) }

    def subQueryOrListPar: Parser[RelSubQueryBase] =
        "(" ~> subQueryOrList <~ ")"
    def subQueryOrList: Parser[RelSubQueryBase] =
        relExpr  ^^ { relExpr => RelSubQuery(relExpr) } |
        scalBaseExprList ^^ { scalars => ScalarList(scalars) }
    def subQueryPar: Parser[RelExpr] = relExprPar

    def scalBaseExprOrListPar: Parser[List[ScalColValue]] =
         scalBaseExpr ^^ { expr => List(expr) } | scalBaseExprListPar
    def scalBaseExprListPar: Parser[List[ScalColValue]] =
        "(" ~> scalBaseExprList <~ ")"
    def scalBaseExprList: Parser[List[ScalColValue]] =
        rep1sep(scalBaseExpr, ",")
    def scalBaseExpr: Parser[ScalColValue] =
        scalExpr ^^ { expr => scalExprEvaluator.eval(expr) }

    def scalExprOrListPar: Parser[List[ScalExpr]] =
         scalExpr ^^ { expr => List(expr) } | scalExprListPar
    def scalExprListPar: Parser[List[ScalExpr]] = "(" ~> scalExprList <~ ")"
    def scalExprList: Parser[List[ScalExpr]] = repsep(scalExpr, ",")
    def scalExprNonEmptyList: Parser[List[ScalExpr]] = rep1sep(scalExpr, ",")
    def scalExprPar: Parser[ScalExpr] = "(" ~> (scalExpr | scalRelExpr) <~ ")"

    override def scalExpr: Parser[ScalExpr] = predExpr

    def predExpr: Parser[ScalExpr] =
        predConjunctExpr ~ rep("OR" ~> predConjunctExpr) ^^ {
            case expr~exprs =>
                exprs.foldLeft (expr) {
                    (expr, nxt) => ScalOpExpr(Or, List(expr, nxt))
                }
        }

    def predConjunctExpr: Parser[ScalExpr] =
        predUnaryExpr ~ rep("AND" ~> predUnaryExpr) ^^ {
            case expr~exprs =>
                exprs.foldLeft (expr) {
                    (expr, nxt) => ScalOpExpr(And, List(expr, nxt))
                }
        }

    def predUnaryExpr: Parser[ScalExpr] =
        ("NOT" ~> predElem) ^^ {
            expr => ScalOpExpr(Not, List(expr))
        } |
        predElem

    def predElem: Parser[ScalExpr] =
        ("EXISTS" ~> subQueryPar) ^^ {
            subQuery => Exists(subQuery)
        } |
        scalArithExpr ~ opt(scalTestPred) ^^ {
            case expr ~ Some(test) => test(expr)
            case expr ~ None => expr
        }

    def scalTestPred: Parser[ScalExpr => ScalExpr] =
        "IS" ~> scalTestPred |
        ("NOT" | "!") ~> scalTestPred ^^ {
            test => (expr: ScalExpr) => test(expr) match {
                case ScalOpExpr(Not, List(inp)) => inp
                case ScalOpExpr(Equals, List(inp)) =>
                    ScalOpExpr(NotEquals, List(inp))
                case ScalOpExpr(NotEquals, List(inp)) =>
                    ScalOpExpr(Equals, List(inp))
                case ScalOpExpr(LessThan, List(inp)) =>
                    ScalOpExpr(GreaterThanEq, List(inp))
                case ScalOpExpr(LessThanEq, List(inp)) =>
                    ScalOpExpr(GreaterThan, List(inp))
                case ScalOpExpr(GreaterThan, List(inp)) =>
                    ScalOpExpr(LessThanEq, List(inp))
                case ScalOpExpr(GreaterThanEq, List(inp)) =>
                    ScalOpExpr(LessThan, List(inp))
                case inp => ScalOpExpr(Not, List(inp))
            }
        } |
        "ISNULL" ^^^ {
            (expr: ScalExpr) => ScalOpExpr(IsNull, List(expr))
        } |
        "NOTNULL" ^^^ {
            (expr: ScalExpr) =>
                ScalOpExpr(Not, List(ScalOpExpr(IsNull, List(expr))))
        } |
        scalCmpOpInfix ~ scalCmpExpr ^^ {
            case op~rhsExpr =>
                (lhsExpr: ScalExpr) => ScalOpExpr(op, List(lhsExpr, rhsExpr))
        } |
        "NULL" ^^^ {
            (expr: ScalExpr) => ScalOpExpr(IsNull, List(expr))
        } |
        "TRUE" ^^^ { // redundant
            (expr: ScalExpr) => expr
        } |
        "FALSE" ^^^ { // negation
            (expr: ScalExpr) => ScalOpExpr(Not, List(expr))
        } |
        "IN" ~> subQueryOrListPar ^^ {
            subQueryOrList => (expr: ScalExpr) =>
                ScalOpExpr(Equals,
                           List(expr, ScalCmpRelExpr(CmpAny, subQueryOrList)))
        } |
        "DISTINCT" ~> "FROM" ~> scalElem ^^ {
            rhsExpr => (lhsExpr: ScalExpr) => ScalOpExpr(IsDistinctFrom,
                                                         List(lhsExpr, rhsExpr))
        } |
        "BETWEEN" ~> rangeQual ~ scalElem ~ ("AND" ~> scalElem) ^^ {
            case qual~lhs~rhs =>
                (expr: ScalExpr) =>
                    ScalOpExpr(IsBetween(qual), List(expr, lhs, rhs))
        } |
        scalLikeOp ^^ {
            op => (expr: ScalExpr) => ScalOpExpr(op, List(expr))
        }


    def scalCmpOpInfix: Parser[ScalRelCmpOp] =
        "=" ^^^ { Equals } |
        "<" ~> opt("=" | ">") ^^ {
            case None => LessThan
            case Some("=") => LessThanEq
            case Some(">") => NotEquals
            case Some(x) => throw new RuntimeException("Unexpected input: " + x)
        } |
        ">" ~> opt("=") ^^ {
            case None => GreaterThan
            case Some("=") => GreaterThanEq
            case Some(x) => throw new RuntimeException("Unexpected input: " + x)
        }

    def scalCmpExpr: Parser[ScalExpr] =
        scalArithExpr |
        subQueryCmpQual ~ subQueryOrListPar ^^ {
            case qual~subQueryOrList => ScalCmpRelExpr(qual, subQueryOrList)
        }

    def subQueryCmpQual: Parser[CmpQual] =
        ("ANY" | "SOME") ^^^ { CmpAny } |
        "ALL" ^^^ { CmpAll }

    def rangeQual: Parser[RangeQual] =
        opt("SYMMETRIC") ^^ {
            case Some("SYMMETRIC") => Symmetric
            case None => Asymmetric
            case Some(x) => throw new RuntimeException("Unexpected input: " + x)
        }

    def scalLikeOp: Parser[PatternMatchOp] =
        "LIKE" ~> pattern ^^ { pat => IsLike(pat) } |
        "ILIKE" ~> pattern ^^ { pat => IsILike(pat) } |
        ("~" | ("SIMILAR" ~ "TO")) ~> pattern  ^^ { pat => IsSimilarTo(pat) }

    def pattern: Parser[Pattern] =
        rawCharConst ~ opt("ESCAPE" ~> rawCharConst) ^^ {
            case pat~Some(escChar) => Pattern(pat, escChar)
            case pat~None => Pattern(pat, "\\")
        }

    def scalArithExpr: Parser[ScalExpr] =
        scalAddArithExpr ~ rep("-" ~> scalAddArithExpr) ^^ {
            case expr~exprs =>
                exprs.foldLeft (expr) {
                    (expr, nxt) => ScalOpExpr(Minus, List(expr, nxt))
                }
        }

    def scalAddArithExpr: Parser[ScalExpr] =
        scalMultArithExpr ~ rep("+" ~> scalMultArithExpr) ^^ {
            case expr~exprs =>
                exprs.foldLeft (expr) {
                    (expr, nxt) => ScalOpExpr(Plus, List(expr, nxt))
                }
        }

    def scalMultArithExpr: Parser[ScalExpr] =
        scalDivArithExpr ~ rep("*" ~> scalDivArithExpr) ^^ {
            case expr~exprs =>
                exprs.foldLeft (expr) {
                    (expr, nxt) => ScalOpExpr(Mult, List(expr, nxt))
                }
        }

    def scalDivArithExpr: Parser[ScalExpr] =
        scalModArithExpr ~ rep("/" ~> scalModArithExpr) ^^ {
            case expr~exprs =>
                exprs.foldLeft (expr) {
                    (expr, nxt) => ScalOpExpr(Div, List(expr, nxt))
                }
        }

    def scalModArithExpr: Parser[ScalExpr] =
        scalExpArithExpr ~ rep("%" ~> scalExpArithExpr) ^^ {
            case expr~exprs =>
                exprs.foldLeft (expr) {
                    (expr, nxt) => ScalOpExpr(Modulo, List(expr, nxt))
                }
        }

    def scalExpArithExpr: Parser[ScalExpr] =
        scalUnaryArithExpr ~ rep("^" ~> scalUnaryArithExpr) ^^ {
            case expr~exprs =>
                exprs.foldLeft (expr) {
                    (expr, nxt) => ScalOpExpr(Exp, List(expr, nxt))
                }
        }

    def scalUnaryArithExpr: Parser[ScalExpr] =
        opt(scalArithOpPrefix) ~ scalConcatElem ^^ {
            case Some(op)~expr => ScalOpExpr(op, List(expr))
            case None~expr => expr
        }

    def scalArithOpPrefix: Parser[UnaryPrefixArithOp] =
        "+" ^^^ { UnaryPlus } | "-" ^^^ { UnaryMinus }

    def scalConcatElem: Parser[ScalExpr] =
        scalTypedElem ~ rep("||" ~> scalTypedElem) ^^ {
            case expr~Nil => expr
            case expr~exprs => ScalOpExpr(ScalarFunction("CONCAT"), expr::exprs)
        }

    def scalTypedElem: Parser[ScalExpr] = 
        "CAST" ~> "(" ~> scalExpr ~ ("AS" ~> sqlType) <~ ")" ^^ {
            case expr~t => ScalOpExpr(TypeCast(t.option), List(expr))
        } |
        scalElem ~ rep("::" ~> sqlType) ^^ {
            case expr~ts =>
                ts.foldLeft (expr) { case (prev, t) =>
                    ScalOpExpr(TypeCast(t.option), List(prev))
                }
        }

    def scalElem: Parser[ScalExpr] =
        scalExprPar | caseExpr | funcOrColRef | scalBaseElem |
        sqlType ~ (scalValueBaseElem | scalExprPar) ^^ {
            case t~expr => ScalOpExpr(TypeCast(t.option), List(expr))
        }

    def caseExpr: Parser[ScalExpr] =
        "CASE" ~> caseArg ~ caseWhenClauseList ~ caseElse <~ "END" ^^ {
            case caseArg~caseWhenClauseList~caseElse =>
                CaseExpr(caseArg, caseWhenClauseList, caseElse)
        }

    def caseArg: Parser[ScalExpr] =
        opt(scalExpr) ^^ {
            case Some(scalExpr) => scalExpr
            case None => BoolConst(true)
        }

    def caseWhenClauseList: Parser[List[(ScalExpr, ScalExpr)]] =
        rep1(caseWhenClause)

    def caseWhenClause: Parser[(ScalExpr, ScalExpr)] =
        "WHEN" ~> scalExpr ~ ("THEN" ~> scalExpr) ^^ {
            case whenExpr~thenExpr => (whenExpr, thenExpr)
        }

    def caseElse: Parser[ScalExpr] =
        opt("ELSE" ~> scalExpr) ^^ {
            case Some(expr) => expr
            case None => SqlNull(SqlCharVarying(None))
        }

    def funcOrColRef: Parser[ScalExpr] =
        ident ~ funcOrColRefPostFix ^^ {
            // funcColGen has the context to create function or column ref
            case arg~funcColGen => funcColGen(arg)
        } |
        "LABEL" ~> labelListPar ~
        ("." ~> matchFnName) ~ ("(" ~> funcArgList <~ ")") ^^ {
            case labels~funcName~funcArgList =>
                ScalOpExpr(LabeledFunction(funcName, labels), funcArgList)
        } |
        "EXTERNAL" ~> ident ~ ("." ~> ident) ~
        ("(" ~> funcArgList <~ ")") ^^ {
            case name~funcName~funcArgList =>
                ScalOpExpr(
                    ExternalScalarFunction(
                        ExternalFunction(name, funcName)
                    ),
                    funcArgList
                )
        } |
        "PG_CATALOG" ~> "." ~> ident ~ ("(" ~> funcArgList <~ ")") ^^ {
            case funcName~funcArgList =>
                ScalOpExpr(pgCatalog.function(funcName), funcArgList)
        }

    def funcOrColRefPostFix: Parser[String => ScalExpr] =
        "(" ~> opt(funcQual) ~ funcArgList <~ ")" ^^ {
            case None~funcArgList => {
                case funcName@(
                    "PG_GET_USERBYID" | "PG_TABLE_IS_VISIBLE" | "PG_GET_EXPR" |
                    "FORMAT_TYPE"
                ) =>
                    ScalOpExpr(pgCatalog.function(funcName), funcArgList)
                case (funcName: String) =>
                    ScalOpExpr(Function(funcName, None), funcArgList)
            } : (String => ScalOpExpr)

            case funcQualOpt~funcArgList =>
                // qualifier, arguments of a function appear now
                // name appears before the expression
                (funcName: String) =>
                    ScalOpExpr(Function(funcName, funcQualOpt), funcArgList)
        } |
        ("[" ~> signedRawIntConst <~ "]") ~ ("." ~> ident) ^^ {
            case index~cRef =>
                // index and column appears now, table before the expression
                (label: String) => LabeledColRef(List(label), Some(index), cRef)
        } |
        opt("." ~> matchFnName ~ opt("(" ~> funcArgList <~ ")")) ^^ {
            case Some(funcName~Some(funcArgList)) =>
                (label: String) =>
                    ScalOpExpr(
                        LabeledFunction(funcName, List(label)), funcArgList
                    )
            case Some(colName~None) =>
                // column appears now, table before the expression
                (label: String) =>
                    LabeledColRef(List(label), None, colName)
            case None =>
                // column appears before the expression, table does not appear
                (colName: String) => ColRef(colName)
        }

    def funcQual: Parser[FuncQual] =
        "ALL" ^^^ { FuncAll } |
        "DISTINCT" ^^^ { FuncDistinct }

    def funcArgList: Parser[List[ScalExpr]] =
        "*" ^^^ { List(IntConst(1)) } |
        scalExprList

    def colRefOrListPar: Parser[List[ColRef]] =
        colRef ^^ { cref => List(cref) } | colRefListPar
    def colRefListOptPar: Parser[List[ColRef]] = colRefList | colRefListPar
    def colRefListParOpt: Parser[List[ColRef]] =
        opt(colRefListPar) ^^ { colsOpt => colsOpt getOrElse Nil }
    def colRefListPar: Parser[List[ColRef]] = "(" ~> colRefList <~ ")"
    def colRefList: Parser[List[ColRef]] = rep1sep(colRef, ",")
    def colRefPar: Parser[ColRef] = "(" ~> colRef <~ ")"
    def colRef: Parser[ColRef] =
        (ident | rawCharConst) ^^ { s => ColRef(s) } 

    def sqlNull: Parser[SqlNull] = "NULL" ^^^ { SqlNull(SqlCharVarying(None)) }

    def numericConst: Parser[NumericConst] =
        numericLit ~ opt("." ~> numericLit) ^^ {
            case whole~Some(frac) =>
                DoubleConst((whole + "." + frac).toDouble)
            case whole~None =>
                LongConst(whole.toLong)
        }

    def charConst: Parser[CharConst] =
        rawCharConst ^^ { s => CharConst(s) }

    def boolConst: Parser[BoolConst] =
        rawBoolConst ^^ { b => BoolConst(b) }

    def signedRawNumericConst: Parser[Double] =
        opt(scalArithOpPrefix) ~ rawNumericConst ^^ {
            case (None | Some(UnaryPlus))~s => s
            case Some(UnaryMinus)~s => -s
        }

    def rawNumericConst: Parser[Double] =
        numericLit ~ opt("." ~> numericLit) ^^ {
            case whole~Some(frac) =>
                (whole + "." + frac).toDouble
            case whole~None =>
                whole.toDouble
        }

    def signedRawIntConst: Parser[Int] =
        opt(scalArithOpPrefix) ~ numericLit ^^ {
            case (None | Some(UnaryPlus))~s =>
                ScalCastEvaluator.toInt(s.toDouble)
            case Some(UnaryMinus)~s =>
                ScalCastEvaluator.toInt(-s.toDouble)
        }

    def rawIntConst: Parser[Int] =
        numericLit ^^ { s => ScalCastEvaluator.toInt(s.toDouble) }
    def rawCharConst: Parser[String] = stringLit
    def rawBoolConst: Parser[Boolean] =
        "TRUE" ^^^ { true } | "FALSE" ^^^ { false }

    def scalBaseElem: Parser[ScalColValue] = scalValueBaseElem | sqlNull

    def scalValueBaseElem: Parser[ScalValueBase] =
        numericConst | charConst | boolConst

    override def sqlType: Parser[SqlType] =
        ("INT" | "INTEGER") ^^^ { SqlType(Types.INTEGER) } |
        "SMALLINT" ^^^ { SqlType(Types.SMALLINT) } |
        "BIGINT" ^^^ { SqlType(Types.BIGINT) } |
        "REAL" ^^^ { SqlType(Types.REAL) } |
        "FLOAT" ~> opt("(" ~> rawIntConst <~ ")") ^^ {
            precOpt => SqlType(Types.FLOAT, None, precOpt)
        } |
        ("DECIMAL" | "NUMERIC" ) ~> 
                opt("(" ~> rawIntConst ~ opt("," ~> rawIntConst) <~ ")") ^^ {
            case Some(prec~scaleOpt) =>
                SqlType(Types.DECIMAL, None, Some(prec), scaleOpt)
            case None =>
                SqlType(Types.DECIMAL)
        } |
        ("BOOL" | "BOOLEAN") ^^^ { SqlType(Types.BOOLEAN) } |
        ("CHAR" | "BPCHAR") ~> opt("VARYING") ~
                opt("(" ~> rawIntConst <~ ")") ^^ {
            case Some(_)~lenOpt =>
                SqlType(Types.NVARCHAR, lenOpt)
            case None~lenOpt =>
                SqlType(Types.NCHAR, lenOpt)
        } |
        ("VARCHAR" | "VARCHAR2") ~> opt("(" ~> rawIntConst <~ ")") ^^ {
            lenOpt => SqlType(Types.NVARCHAR, lenOpt)
        } |
        "TEXT" ^^^ { SqlType(Types.LONGVARCHAR) } |
        "TIMESTAMP" ~> opt("WITHOUT" <~ "TIME" <~ "ZONE") ^^^ {
            SqlType(Types.TIMESTAMP)
        } |
        "TIME" ^^^ { SqlType(Types.TIME) } |
        "DATE" ^^^ { SqlType(Types.DATE) } |
        "PG_CATALOG" ~> "." ~> pgCatalogType

    def pgCatalogType: Parser[SqlType] =
        ident ^^^ { SqlCharVarying(None) } |
        sqlType

    def extRelOp: Parser[ExtendedRelOp] =
        "CLASSIFIED" ~> opt("WITH") ~> ident ~ colRefPar ^^ {
            case cid~col => Classify(MLObjectId(cid), col)
        } |
        "CLUSTERED" ~> opt("WITH") ~> ident ~ colRefPar ^^ {
            case cid~col => Cluster(MLObjectId(cid), col)
        } |
        rep1sep("IMPUTED" ~> opt("WITH") ~>
                ident ~ colRefPar ~ opt("FLAG" ~> colRef), ",") ^^ { cidCols =>
            val imputeSpecs: List[ImputeSpec] = cidCols.map {
                case cid~col~flagColOpt =>
                    ImputeSpec(col, flagColOpt, MLObjectId(cid))
            }

            Impute(imputeSpecs)
        } |
        "TEXT" ~> opt("(" ~> rawCharConst <~ ")") ~ annotIdent ~
        opt(scalExprOrListPar) ~ ("IN" ~> colRef) ~
        opt("TO" ~> colRefOrListPar) ^^ {
            case langOpt~((libOpt, op))~argsOpt~inpCol~resColsOpt =>
                val args: List[ScalExpr] = argsOpt.toList.flatten
                val resCols: List[ColRef] = resColsOpt.toList.flatten
                NlpRelOp(libOpt, langOpt, op, args, inpCol, resCols)
        } |
        "UNPIVOT" ~> colRef ~ ("FOR" ~> colRef) ~ ("IN" ~> "(" ~>
            rep1sep(colRef ~ opt("AS" ~> charConst), ",") <~
        ")") ^^ {
            case outValCol~outKeyCol~inColValPairs =>
                val inColVals: List[(ColRef, CharConst)] =
                    inColValPairs.map { case col~vOpt =>
                        (col, vOpt getOrElse CharConst(col.name))
                    }

                UnPivot(outValCol, outKeyCol, inColVals)
        } |
        "ORDERED" ~> "BY" ~> orderedScalExprOrListPar ^^ {
            sortExprs => OrderedBy(sortExprs)
        }

    def extensionRelExpr: Parser[RelBaseExpr] =
        "EXTERNAL" ~> ident ~ opt("(" ~> repsep(extParam, ",") <~ ")") ^^ {
            case sourceName~params => ExternalSourceExpr(
                schema, ExternalSource(sourceName, params.toList.flatten)
            )
        } |
        "SEQUENCE" ~> "(" ~> rawIntConst <~ ")" ^^ {
            n => ExternalSourceExpr(schema, SequenceSource(n))
        }

    def extParam: Parser[ScalValueBase] =
        ident ^^ { id => CharConst(id) } | scalValueBaseElem

    def matchArgSpecOrParList: Parser[List[SeqAggregateColSpec]] =
        matchArgSpec ^^ { spec => List(spec) } |
        "(" ~> rep1sep(matchArgSpec, ",") <~ ")"

    def matchArgSpec: Parser[SeqFunctionSpec] =
        opt(labelOrMarkedList <~ ".") ~
        matchFnName ~ opt("(" ~> funcArgList <~ ")") ^^ {
            case labelsOpt~fname~argsOpt =>
                SeqFunctionSpec(
                    ColRef(fname), fname, argsOpt getOrElse Nil,
                    labelsOpt.toList.flatten.map { id => Label(id) }
                )
        }

    def matchFnName: Parser[String] = ident | "EXISTS"

    def labelOrListPar: Parser[List[String]] =
        label ^^ { label => List(label) } | labelListPar
    def labelListPar: Parser[List[String]] =
        "(" ~> labelList <~ ")"

    def labelOrMarkedList: Parser[List[String]] =
        label ^^ { label => List(label) } | labelMarkedList
    def labelMarkedList: Parser[List[String]] =
        "LABEL" ~> "(" ~> labelList <~ ")"

    def labelList: Parser[List[String]] = rep1sep(label, ",")
    def label: Parser[String] =
        (ident | rawCharConst | numericLit) ^^ { s => s.toUpperCase }

    def regex: Parser[String] = rawCharConst ^^ { s => s.toUpperCase }

    def annotIdent: Parser[(Option[String], String)] =
        ident ~ opt("." ~> ident) ^^ {
            case annot~Some(base) => (Some(annot), base)
            case base~None => (None, base)
        }
}
