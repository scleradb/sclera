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

import java.io.File

import org.jline.terminal.{TerminalBuilder, Terminal}
import org.jline.reader.{LineReaderBuilder, LineReader, History, Completer}
import org.jline.reader.impl.completer.StringsCompleter
import org.jline.reader.impl.history.DefaultHistory

import scala.jdk.CollectionConverters._

object ReplReader {
    def apply(historyPath: String, keywords: Iterable[String]): LineReader = {
        val historyFile: File = new File(historyPath)
        Option(historyFile.getParentFile()).foreach { d => d.mkdirs() }
        historyFile.createNewFile()

        apply(historyFile, keywords)
    }

    def apply(historyFile: File, keywords: Iterable[String]): LineReader = {
        val terminal: Terminal = TerminalBuilder.builder().build()

        val expandedKeywords: Iterable[String] =
            keywords.map(_.toUpperCase) ++ keywords

        val completer: Completer = new StringsCompleter(expandedKeywords.asJava)

        val history: History = new DefaultHistory()

        LineReaderBuilder.builder()
            .terminal(terminal)
            .completer(completer)
            .variable(LineReader.HISTORY_FILE, historyFile)
            .history(history)
            .build()
    }
}
