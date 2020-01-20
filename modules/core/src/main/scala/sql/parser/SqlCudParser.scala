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

import com.scleradb.objects._

import com.scleradb.sql.objects._
import com.scleradb.sql.datatypes._
import com.scleradb.sql.types._
import com.scleradb.sql.expr._
import com.scleradb.sql.statements._

import com.scleradb.dbms.location._

import com.scleradb.external.objects.ExternalTarget

import com.scleradb.analytics.sequence.labeler.{RowLabeler, ColumnRowLabeler}
import com.scleradb.analytics.ml.objects.MLObjectId

// SQL CUD parser
private[scleradb]
trait SqlCudParser extends SqlQueryParser {
    override def sqlStatement: Parser[SqlStatement] =
        super.sqlStatement | sqlUpdateStatement

    lexical.delimiters ++= Seq(",", ":", "(", ")", "=", "@" )
    lexical.reserved ++= Seq(
        "ALTER", "AS", "CLASSIFIER", "CLUSTERER", "CREATE",
        "DELETE", "DISTINCT", "DROP", "EXTERNAL",
        "FOREIGN", "FROM", "INSERT", "INTO", "KEY",
        "LOCATION", "NOT", "NULL", "ON", "PRIMARY",
        "REFERENCES", "SET", "TABLE", "TEMP", "TEMPORARY",
        "UPDATE", "USING", "VALUES", "VIEW"
    )

    def sqlUpdateStatement: Parser[SqlUpdateStatement] =
        create | insert | update | delete | drop | alter | nativeStatement

    def create: Parser[SqlCreate] = "CREATE" ~> createOpts
    
    def createOpts: Parser[SqlCreate] = createExtTarget | createObjDuration

    def createExtTarget: Parser[SqlCreateExt] =
        "EXTERNAL" ~> ident ~
        opt("(" ~> rep1sep(scalValueBaseElem, ",") <~ ")") ~
        ("AS" ~> relExpr) ^^ {
            case sourceName~params~relExpr =>
                SqlCreateExt(
                    ExternalTarget(sourceName, params.toList.flatten),
                    relExpr
                )
        }

    def createObjDuration: Parser[SqlCreateObj] =
        duration ~ createObj ^^ {
            case duration~obj => obj(duration)
        }

    def duration: Parser[DbObjectDuration] = 
        opt("TEMP" | "TEMPORARY") ^^ {
            case Some(_) => Temporary
            case None => Persistent
        }

    def createObj: Parser[DbObjectDuration => SqlCreateObj] =
        createRelObj ^^ {
            obj => (dur: DbObjectDuration) => SqlCreateDbObject(obj, dur)
        } |
        opt(ident) ~ createMLObj ~ ("USING" ~> relExpr) ~ opt(
            "DISTINCT" ~> opt("VALUES") ~>
            "(" ~> repsep(distinctSpec, ",") <~ ")"
        ) ^^ {
            case libOpt~obj~expr~numDistinctValsOpt =>
                val numDistinctValMap: Map[ColRef, Int] =
                    Map() ++ (numDistinctValsOpt getOrElse Nil)
                (dur: DbObjectDuration) =>
                    SqlCreateMLObject(libOpt, obj(numDistinctValMap), expr, dur)
        }

    def distinctSpec: Parser[(ColRef, Int)] =
        colRef ~ (":" ~> rawIntConst) ^^ {
            case col~numDistinct => (col, numDistinct)
        }

    def createRelObj: Parser[SqlDbObject] =
        "TABLE" ~> annotIdent ~ createTable ^^ {
            case ((locNameOpt, tableName))~tableDef =>
                val locIdOpt: Option[LocationId] =
                    locNameOpt.map { locName => LocationId(locName) }
                tableDef(locIdOpt, tableName)
        } |
        "VIEW" ~> ident ~ createView ^^ {
            case viewName~viewDef => viewDef(viewName)
        }

    def createMLObj: Parser[Map[ColRef, Int] => SqlMLObject] =
        "CLASSIFIER" ~> opt("(" ~> objectSpec <~ ")") ~ ident ~
        ("(" ~> colRef <~ ")") ^^ {
            case classifierSpecOpt~name~targetCol =>
                (numDistinctValsMap: Map[ColRef, Int]) =>
                    SqlClassifier(
                        name, classifierSpecOpt, targetCol, numDistinctValsMap
                    )
        } |
        "CLUSTERER" ~> opt("(" ~> objectSpec <~ ")") ~ ident ^^ {
            case clustererSpecOpt~name =>
                (numDistinctValsMap: Map[ColRef, Int]) =>
                    SqlClusterer(name, clustererSpecOpt, numDistinctValsMap)
        }

    def objectSpec: Parser[(String, String)] =
        rawCharConst ~ opt("," ~> rawCharConst) ^^ {
            case algName~optionsOpt => (algName, optionsOpt getOrElse "")
        }

    def createTable: Parser[(Option[LocationId], String) => SqlDbObject] =
        "AS" ~> relExpr ^^ {
            expr => {
                (locIdOpt: Option[LocationId], name: String) =>
                    SqlObjectAsExpr(name, expr, DbMaterialized(locIdOpt))
            }
        } |
        tableExplicitDef ~ opt("AS" ~> relExpr) ^^ {
            case tableExplicitDef~relExprOpt =>
                (locIdOpt: Option[LocationId], name: String) =>
                    SqlTable(tableExplicitDef(name), locIdOpt, relExprOpt)
        }

    def tableExplicitDef: Parser[String => Table] =
        "(" ~> tableColDefList ~ opt("," ~> tableKeys) <~ ")" ^^ {
            case ((cols, colPks, colFks))~tableKeysOpt => {
                val (tablePks, tableFks) = tableKeysOpt.getOrElse((Nil, Nil))
                val pks: List[PrimaryKey] = (tablePks:::colPks).distinct
                val fks: List[ForeignKey] = (tableFks:::colFks).distinct

                (name: String) =>
                    if( pks.length > 1 )
                        throw new IllegalArgumentException(
                            "Table \"" + name + "\" has multiple primary keys"
                        )
                    else Table(name, cols, pks.headOption, fks, Table.BaseTable)
            }
        }

    def tableKeys: Parser[(List[PrimaryKey], List[ForeignKey])] =
        foreignKeys ~ opt("," ~> tableKeys) ^^ {
            case fks~Some((prevPks, prevFks)) => (prevPks, fks:::prevFks)
            case fks~None => (Nil, fks)
        } |
        primaryKey ~ opt("," ~> tableKeys) ^^ {
            case pk~Some((prevPks, prevFks)) => (pk::prevPks, prevFks)
            case pk~None => (List(pk), Nil)
        }

    def tableColDefList: Parser[
        (List[Column], List[PrimaryKey], List[ForeignKey])
    ] = rep1sep(tableColDef, ",") ^^ { colDefs =>
        colDefs.reverse.foldLeft (List[Column](), List[PrimaryKey](),
                                                  List[ForeignKey]()) {
            case ((cols, pks, fks), (col, colPkOpt, colFks)) =>
                (col::cols, colPkOpt.toList:::pks, colFks:::fks)
        }
    }

    def tableColDef: Parser[(Column, Option[PrimaryKey], List[ForeignKey])] =
        ident ~ sqlType ~ nullQual ~ opt(pkQual) ~ rep(refQual) ^^ {
            case name~sqlType~nullQual~pkQualOpt~tableRefs =>
                val col: Column = Column(name, nullQual(sqlType))
                val pkOpt: Option[PrimaryKey] =
                    pkQualOpt.map { _ =>
                        PrimaryKey(List(ColRef(name)))
                    }
                val fks: List[ForeignKey] =
                    tableRefs.map { case (refLocOpt, refTableName, refCols) =>
                        ForeignKey(
                            List(ColRef(name)),
                            refLocOpt, refTableName, refCols
                        )
                    }

                (col, pkOpt, fks)
        }

    def nullQual: Parser[SqlType => SqlType] =
        opt(opt("NOT") <~ "NULL") ^^ {
            case Some(Some("NOT")) => { sqlType => sqlType.baseType }
            case Some(None) | None => { sqlType => sqlType.option }
            case Some(Some(x)) =>
                throw new RuntimeException("Unexpected input: " + x)
        }

    def primaryKey: Parser[PrimaryKey] =
        pkQual ~> colRefListPar ^^ { colRefs => PrimaryKey(colRefs) }

    def foreignKeys: Parser[List[ForeignKey]] =
        fkQual ~> colRefListPar ~ rep(refQual) ^^ {
            case colRefs~tableRefs =>
                tableRefs.map { case (refTableLocOpt, refTableName, refCols) =>
                    ForeignKey(colRefs, refTableLocOpt, refTableName, refCols)
                }
        }

    def pkQual: Parser[String] = "PRIMARY" <~ "KEY"

    def fkQual: Parser[String] = "FOREIGN" <~ "KEY"

    def refQual: Parser[(Option[LocationId], String, List[ColRef])] =
        "REFERENCES" ~> ident ~ opt(ident) ~ opt(colRefListPar) ^^ {
            case loc~Some(tName)~cols =>
                (Some(LocationId(loc)), tName, cols.toList.flatten)
            case tName~None~cols =>
                (None, tName, cols.toList.flatten)
        }

    def createView: Parser[String => SqlDbObject] =
        "AS" ~> relExpr ^^ {
            expr => (name: String) => SqlObjectAsExpr(name, expr, DbVirtual)
        } |
        ("(" ~> rep1sep(viewColDef, ",") <~ ")") ~ ("AS" ~> relExpr) ^^ {
            case viewCols~relExpr => (name: String) =>
                val aliasCols: List[ColRef] =
                    viewCols.map { col => ColRef(col.name) }
                val aliasedExpr: RelExpr =
                    RelOpExpr(TableAlias(name, aliasCols), List(relExpr))

                val casts: List[AliasedExpr] =
                    viewCols.map { col => 
                        val colRef: ColRef = ColRef(col.name)
                        val cast: ScalExpr =
                            ScalOpExpr(TypeCast(col.sqlType), List(colRef))
                        AliasedExpr(cast, colRef)
                    }

                val castedExpr: RelExpr =
                    RelOpExpr(Project(casts), List(aliasedExpr))

                SqlObjectAsExpr(name, castedExpr, DbVirtual)
        }

    def viewColDef: Parser[Column] =
        ident ~ sqlType ~ nullQual ^^ {
            case name~sqlType~nullQual => Column(name, nullQual(sqlType))
        }

    def insert: Parser[SqlInsert] =
        "INSERT" ~> "INTO" ~> tableId ~ opt(colRefListPar) ~ relExpr ^^ {
            case tableId~colsOpt~relExpr =>
                SqlInsert(tableId, colsOpt.toList.flatten, relExpr)
        }

    def update: Parser[SqlUpdate] =
        "UPDATE" ~> tableId ~ ("SET" ~> rep1(setExpr)) ~ updatePredicate ^^ {
            case tableId~colValList~pred =>
                SqlUpdate(tableId, colValList, pred)
        }

    def setExpr: Parser[(ColRef, ScalExpr)] =
        (colRef <~ "=") ~ scalExpr ^^ {
            case cRef~value => (cRef, value)
        }

    def updatePredicate: Parser[ScalExpr] =
        opt("WHERE" ~> scalExpr) ^^ {
            case Some(expr) => expr
            case None => BoolConst(true)
        }

    def delete: Parser[SqlDelete] =
        "DELETE" ~> "FROM" ~> tableId ~ updatePredicate ^^ {
            case tableId~pred => SqlDelete(tableId, pred)
        }

    def drop: Parser[SqlDrop] =
        "DROP" ~> objectId ^^ {
            case objectId => SqlDropById(objectId)
        }

    def objectId: Parser[SchemaObjectId] =
        "TABLE" ~> tableId |
        "VIEW" ~> ident ^^ { name => ViewId(name) } |
        ("CLASSIFIER" | "CLUSTERER") ~> ident ^^ {
            name => MLObjectId(name)
        }

    def alter: Parser[SqlAlter] = "ALTER" ~> alterOpts

    def alterOpts: Parser[SqlAlter] =
        failure("ALTER statement is not supported")

    def nativeStatement: Parser[SqlNativeStatement] =
        "@" ~> ident ~ rawCharConst ^^ {
            case locName~stmtStr =>
                SqlNativeStatement(LocationId(locName), stmtStr)
        }
}
