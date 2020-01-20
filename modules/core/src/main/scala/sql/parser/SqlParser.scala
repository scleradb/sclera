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

import java.sql.SQLSyntaxErrorException

import scala.util.parsing.input.CharArrayReader
import scala.util.parsing.combinator.syntactical.StdTokenParsers

import com.scleradb.util.parsing.CaseInsensitive

import com.scleradb.exec.Schema

import com.scleradb.sql.expr.{RelExpr, ScalExpr}
import com.scleradb.sql.types.SqlType
import com.scleradb.sql.statements.SqlStatement
import com.scleradb.sql.exec.ScalExprEvaluator

private[scleradb]
abstract class SqlParser(
    val schema: Schema, // associated schema
    val scalExprEvaluator: ScalExprEvaluator // scalar expr evaluator
) extends StdTokenParsers with CaseInsensitive {
    // sql statement
    def sqlStatement: Parser[SqlStatement]

    // relational expressiom
    def relExpr: Parser[RelExpr]

    // scalar expressiom
    def scalExpr: Parser[ScalExpr]

    // SQL data type
    def sqlType: Parser[SqlType]

    lexical.delimiters ++= Seq(";")

    def keywords: Set[String] = lexical.reserved.toSet

    def sqlStatements: Parser[List[SqlStatement]] =
        rep1sep(sqlStatement, ";") <~ opt(";")

    def parseSqlStatements[T](s: String): List[SqlStatement] =
        parse(sqlStatements, s)

    def parseRelExpr(s: String): RelExpr = parse(relExpr, s)
    def parseScalExpr(s: String): ScalExpr = parse(scalExpr, s)
    def parseSqlType(s: String): SqlType = parse(sqlType, s)

    def parse[T](p: Parser[T], s: String): T = {
        val reader: CharArrayReader = new CharArrayReader(s.toArray)
        val tokens = new lexical.Scanner(reader)
        phrase(p)(tokens) match {
            case Success(result, _) => result
            case NoSuccess(msg, in) =>
                throw new IllegalArgumentException(
                    msg + " (" + in.pos + ")\n" +
                    in.pos.longString
                )
        }
    }
}
