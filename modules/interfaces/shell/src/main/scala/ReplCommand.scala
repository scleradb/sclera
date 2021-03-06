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

import com.scleradb.sql.expr.RelExpr
import com.scleradb.sql.statements.{SqlStatement, SqlQueryStatement}

import com.scleradb.visual.model.spec.PlotSpec

abstract class ReplCommand
case class SqlCommand(stmt: SqlStatement) extends ReplCommand

sealed abstract class MetaCommand extends ReplCommand
case class Echo(isEnabled: Boolean) extends MetaCommand
case class OutputFormat(formatOpt: Option[CSVFormat]) extends MetaCommand
case class CommandTimer(command: ReplCommand) extends MetaCommand
case class Source(fileName: String) extends MetaCommand
case object Reset extends MetaCommand

sealed abstract class DisplayCommand extends ReplCommand
case class DisplayStart(args: List[String]) extends DisplayCommand
case class DisplayStop(args: List[String]) extends DisplayCommand
case class DisplayResult(
    query: RelExpr,
    specOpt: Option[PlotSpec],
    titleOpt: Option[String]
) extends DisplayCommand
case class DisplayText(text: String) extends DisplayCommand
