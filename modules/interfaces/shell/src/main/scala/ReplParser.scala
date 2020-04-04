/**
* Sclera - Shell
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

package com.scleradb.interfaces.shell

import org.apache.commons.csv.CSVFormat

import com.scleradb.sql.statements.{SqlRelQueryStatement, SqlAddLocation}
import com.scleradb.sql.parser.{SqlQueryParser, SqlAdminParser}

import com.scleradb.visual.model.spec.PlotSpec
import com.scleradb.visual.sql.parser.PlotParser

trait ReplParser extends SqlQueryParser with SqlAdminParser with PlotParser {
    lexical.delimiters ++= Seq(";")
    lexical.reserved ++= Seq(
        "DISPLAY", "ECHO", "FORMAT", "RESET", "SOURCE", "START", "STOP", "TIME"
    )

    def commands: Parser[List[ReplCommand]] =
        repsep(opt(command), ";") ^^ { cs => cs.flatten }

    def command: Parser[ReplCommand] =
        sqlStatement ~ opt(plotSpec) ^^ {
            case SqlRelQueryStatement(query)~(plotSpecOpt@Some(_)) =>
                DisplayResult(query, plotSpecOpt, titleOpt = None)
            case stmt ~ _ =>
                SqlCommand(stmt)
        } |
        "DISPLAY" ~> displayCommand |
        "TIME" ~> command ^^ { command => CommandTimer(command) } |
        "SOURCE" ~> stringLit ^^ { filename => Source(filename) } |
        "FORMAT" ~> ("TABLE" | ident) ~ opt(ident) ^^ {
            case "TABLE"~_ =>
                OutputFormat(None)
            case "CSV"~None =>
                OutputFormat(Some(CSVFormat.DEFAULT))
            case "CSV"~Some("DEFAULT") =>
                OutputFormat(Some(CSVFormat.DEFAULT))
            case "CSV"~Some("EXCEL") =>
                OutputFormat(Some(CSVFormat.EXCEL))
            case "CSV"~Some("MYSQL") =>
                OutputFormat(Some(CSVFormat.MYSQL))
            case "CSV"~Some("RFC4180") =>
                OutputFormat(Some(CSVFormat.RFC4180))
            case "CSV"~Some("TDF") =>
                OutputFormat(Some(CSVFormat.TDF))
            case _ =>
                throw new IllegalArgumentException("Incorrect format")
        } |
        "ECHO" ~> switch ^^ { isEnabled => Echo(isEnabled) } |
        "RESET" ^^^ { Reset } |
        failure("Incorrect command")

    private def displayCommand: Parser[DisplayCommand] =
        ("START" | "STOP") ~ rep(ident | rawCharConst | numericLit) ^^ {
            case "START"~params => DisplayStart(params)
            case "STOP"~params => DisplayStop(params)
            case _ => throw new IllegalArgumentException("Incorrect command")
        } |
        opt(paren(rawCharConst)) ~ sqlQueryStatement ~ opt(plotSpec) ^^ {
            case titleOpt~SqlRelQueryStatement(query)~plotSpecOpt =>
                DisplayResult(query, plotSpecOpt, titleOpt)
        } |
        rawCharConst ^^ { text => DisplayText(text) }

    private def switch: Parser[Boolean] =
        "ON" ^^^ { true } | "OFF" ^^^ { false }

    override def addLocation: Parser[SqlAddLocation] = super.addLocation ^^ {
        case SqlAddLocation(id, dbname, params, dbSchemaOpt, permitStrOpt) =>
            val param: String = params.headOption getOrElse {
                throw new IllegalArgumentException(
                    "Location \"" + id + "\": Parameters not specified"
                )
            }

            // parse the key/value pairs
            val config: List[(String, String)] = params.tail.map { str =>
                str.split("=") match {
                    case Array(k, v) =>
                        (k.trim, v.trim)
                    case _ =>
                        throw new IllegalArgumentException(
                            "Location \"" + id + "\": " +
                            "Invalid parameter: \"" + str + "\""
                        )
                }
            } map { case (k, v) => (k, Repl.replaceToken(k + ": ", v)) }

            val updParams: List[String] =
                param::(config.map { case (k, v) => k + "=" + v })

            SqlAddLocation(
                id, dbname, updParams, dbSchemaOpt, permitStrOpt
            )
    }
}
