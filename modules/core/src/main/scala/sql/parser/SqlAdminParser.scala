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

import com.scleradb.sql.statements._
import com.scleradb.sql.objects.TableId
import com.scleradb.dbms.location._

// SQL ADMIN parser
trait SqlAdminParser extends SqlQueryParser with SqlCudParser {
    override def sqlStatement: Parser[SqlStatement] = super.sqlStatement |
        sqlAdminStatement | sqlAdminQueryStatement | sqlExplainStatement

    lexical.delimiters ++= Seq(",", "(", ")", "=", ".", "*" )
    lexical.reserved ++= Seq(
        "ADD", "CLASSIFIER", "CLASSIFIERS", "CLUSTERER", "CLUSTERERS",
        "DESCRIBE", "DROP", "EXPLAIN", "FOR",
        "LIST", "LOCATION", "LOCATIONS", "OFF", "ON",
        "READONLY", "REMAINING", "REMOVE", "SCHEMA", "SCRIPT", "SET", "SHOW",
        "TABLE", "TABLES", "VIEW", "VIEWS"
    )

    def sqlAdminStatement: Parser[SqlAdminStatement] =
        createSchema | dropSchema |
        addStatement | removeStatement | configStatement

    def createSchema: Parser[SqlAdminStatement] =
        "CREATE" ~> "SCHEMA" ^^^ { SqlCreateSchema }
    
    def dropSchema: Parser[SqlAdminStatement] =
        "DROP" ~> "SCHEMA" ^^^ { SqlDropSchema }
    
    def addStatement: Parser[SqlAdminStatement] =
        "ADD" ~> (addLocation | addTable)

    def addLocation: Parser[SqlAddLocation] =
        opt("READONLY") ~ ("LOCATION" ~> ident) ~
        ("AS" ~> ident) ~ ("(" ~> rep1sep(rawCharConst, ",") <~ ")") ~
        opt("SCHEMA" ~> rawCharConst) ^^ {
            case permitStrOpt~locName~dbname~params~dbSchemaOpt =>
                SqlAddLocation(
                    LocationId(locName), dbname,
                    params, dbSchemaOpt, permitStrOpt
                )
        }

    def addTable: Parser[SqlAddTable] =
        "TABLE" ~> annotIdent ~ opt(tableExplicitDef) ^^ {
            case ((locNameOpt, tName))~tableExplicitDefOpt =>
                val locationId: LocationId = locNameOpt match {
                    case Some(locName) => LocationId(locName)
                    case None => Location.defaultLocationId
                }

                SqlAddTable(
                    TableId(locationId, tName),
                    tableExplicitDefOpt.map { tDef => tDef(tName) }
                )
        }

    def removeStatement: Parser[SqlAdminStatement] =
        "REMOVE" ~> (removeLocation | removeTable)

    def removeLocation: Parser[SqlRemoveLocation] =
        "LOCATION" ~> ident ^^ {
            locName => SqlRemoveLocation(LocationId(locName))
        }

    def removeTable: Parser[SqlRemoveTable] =
        "TABLE" ~> tableId ^^ { tid => SqlRemoveTable(tid) }

    def configStatement: Parser[SqlAdminStatement] = "SET" ~> configLocation

    def configLocation: Parser[SqlConfigLocation] =
        (ident <~ "LOCATION" <~ "=") ~ ident ^^ {
            case param~locName => SqlConfigLocation(param, LocationId(locName))
        }

    def user: Parser[String] = stringLit | ident

    def sqlAdminQueryStatement: Parser[SqlAdminQueryStatement] =
        "SHOW" ~> showOption |
        "LIST" ~> listOption(ShortFormat) |
        "DESCRIBE" ~> listOption(LongFormat) |
        failure("Incorrect command")

    def showOption: Parser[SqlAdminQueryStatement] = ident ^^ {
        case "OPTIONS" => SqlShowOptions
        case "CONFIG" => SqlShowConfig
        case _ => throw new IllegalArgumentException("Incorrect SHOW option")
    }

    def listOption(format: Format): Parser[SqlAdminQueryStatement] =
        "REMAINING" ~> opt(ident) ^^ {
            locOpt =>
                val locIdOpt: Option[LocationId] =
                    locOpt.map { loc => LocationId(loc) }
                SqlListRemainingTables(locIdOpt, format)
        } |
        ("TABLE" | "TABLES") ~> opt(ident) ~ opt("." ~> (ident | "*")) ^^ {
            case nameOpt~None =>
                SqlListAddedTables(None, nameOpt, format)
            case Some(loc)~Some("*") =>
                SqlListAddedTables(Some(LocationId(loc)), None, format)
            case locOpt~nameOpt =>
                val locIdOpt: Option[LocationId] =
                    locOpt.map { loc => LocationId(loc) }
                SqlListAddedTables(locIdOpt, nameOpt, format)
        } |
        ("VIEW" | "VIEWS") ~> opt(ident) ^^ {
            nameOpt => SqlListViews(nameOpt, format)
        } |
        ("CLASSIFIER" | "CLASSIFIERS") ~> opt(ident) ^^ {
            nameOpt => SqlListClassifiers(nameOpt, format)
        } |
        ("CLUSTERER" | "CLUSTERERS") ~> opt(ident) ^^ {
            nameOpt => SqlListClusterers(nameOpt, format)
        } |
        ("LOCATION" | "LOCATIONS") ^^^ {
            SqlListLocations
        } |
        opt(ident ~ opt("." ~> (ident | "*"))) ^^ {
            case Some(loc~Some("*")) =>
                SqlListAddedTables(Some(LocationId(loc)), None, format)
            case Some(loc~Some(name)) =>
                SqlListAddedTables(Some(LocationId(loc)), Some(name), format)
            case Some(name~None) =>
                SqlListObjects(Some(name), format)
            case None =>
                SqlListObjects(None, format)
        } |
        failure("Incorrect LIST option")

    def sqlExplainStatement: Parser[SqlStatement] =
        "EXPLAIN" ~> explainOption

    def explainOption: Parser[SqlStatement] =
        "SCRIPT" ~> opt(explainScriptOption) ^^ {
            case Some(isExplain) => SqlExplainScript(isExplain)
            case None => SqlExplainScript(true)
        } |
        relExpr ^^ { expr => SqlExplainPlan(expr) } |
        failure("Incorrect EXPLAIN option")

    def explainScriptOption: Parser[Boolean] =
        "ON" ^^^ { true } |
        "OFF" ^^^ { false } |
        failure("Incorrect EXPLAIN SCRIPT option")
}
