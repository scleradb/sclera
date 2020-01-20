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

import java.io.IOException
import java.lang.{Character, StringBuilder}
import java.sql.{SQLWarning, SQLException}

import org.slf4j.{Logger, LoggerFactory}
import ch.qos.logback.classic.LoggerContext
import ch.qos.logback.core.util.StatusPrinter

import org.jline.reader.LineReader
import org.jline.reader.EndOfFileException

import org.apache.commons.csv.{CSVFormat, CSVPrinter}

import scala.collection.mutable

import com.scleradb.exec.Processor

import com.scleradb.config.ScleraConfig
import com.scleradb.util.tools.{Counter, Format}

import com.scleradb.sql.statements._
import com.scleradb.sql.parser._
import com.scleradb.sql.expr.{CharConst, DateConst, TimeConst, TimestampConst}
import com.scleradb.sql.result.TableResult

import com.scleradb.visual.exec.{PlotProcessor, PlotRender}

import com.scleradb.interfaces.display.Display
import com.scleradb.interfaces.display.service.DisplayService

object Repl {
    private val logger: Logger = LoggerFactory.getLogger(this.getClass.getName)

    private def logException(
        e: Throwable,
        handler: (String, Throwable) => Unit,
        annotOpt: Option[String] = None
    ): Unit = {
        val annot: String = annotOpt getOrElse "Exception"
        handler(annot, e)

        Option(e.getMessage).foreach { s =>
            Console.err.println(s"[$annot] $s")
        }
    }

    private var displayOpt: Option[Display] = None
    private def display: Display = displayOpt getOrElse {
        throw new RuntimeException("Display not started")
    }

    private var processorOpt: Option[Processor] = None
    private def processor: Processor = processorOpt getOrElse {
        throw new RuntimeException("Sclera not initialized. Exiting.")
    }

    private lazy val plotProcessor: PlotProcessor = PlotProcessor(processor)

    private var outputFormatOpt: Option[CSVFormat] = None
    private var isEchoEnabled: Boolean = true

    private lazy val replParser: ReplParser =
        new SqlParser(processor.schema, processor.scalExprEvaluator)
        with SqlQueryParser with SqlCudParser
        with SqlAdminParser with ReplParser

    private lazy val replReader: LineReader = ReplReader(
        historyPath = ScleraConfig.historyPath,
        keywords = replParser.lexical.reserved
    )

    private def scriptIter(
        input: Iterator[String],
        partial: Option[String] = None
    ): Iterator[String] =
        if( input.hasNext ) {
            val s: String = input.next.trim
            if( s == "" || s.take(2) == "--" ) // empty/comment
                scriptIter(input, partial)
            else {
                val newp: String = partial match {
                    case Some(p) => p + " " + s
                    case None => s
                }

                s.last match {
                    case ';' => // input complete
                        Iterator(newp) ++ scriptIter(input, None)
                    case _ => // input incomplete
                        scriptIter(input, Some(newp))
                }
            } 
        } else Iterator.empty

    private def consoleIter(
        inpPrompt: String,
        handler: String => Unit,
        partialOpt: Option[String] = None
    ): Unit = {
        val prompt: String = if( isEchoEnabled ) inpPrompt else ""
        val echoChar: Character =
            if( isEchoEnabled ) null else Character.MIN_VALUE

        Option(replReader.readLine(prompt, echoChar)).map(s => s.trim).foreach {
            case s if( s == "" || s.take(2) == "--" ) => // empty/comment
                consoleIter(inpPrompt, handler, partialOpt)

            case s =>
                val ps: String = partialOpt match {
                    case Some(p) => p + "\n" + s
                    case None => s
                }

                s.last match {
                    case ';' => // input complete
                        handler(ps)
                        consoleIter(ScleraConfig.prompt, handler, None)
                    case _ => // input incomplete
                        consoleIter(ScleraConfig.partPrompt, handler, Some(ps))
                }
        }
    }

    def replaceToken(prompt: String, str: String): String =
        if( str.trim == ScleraConfig.inputToken )
            Option(replReader.readLine(prompt, '*')) getOrElse ""
        else str

    def handleInput(inpStr: String): Unit =
        replParser.parse(replParser.commands, inpStr).foreach(handleCommand)

    private def handleInputInteractive(inpStr: String): Unit =
        try handleInput(inpStr) catch {
            case (e: IllegalArgumentException) =>
                logException(e, logger.info)
            case (e: SQLException) =>
                logException(e, logger.info, Some("SQL Exception"))
            case (e: IOException) =>
                logException(e, logger.info, Some("IO Exception"))
            case (e: Throwable) =>
                logException(e, logger.error, Some("Internal Error"))
        }

    private def handleCommand(command: ReplCommand): Unit = command match {
        case SqlCommand(stmt) =>
            Counter.reset()
            handleSqlStatement(stmt)

        case DisplayStart(args) =>
            val display: Display = DisplayService().createDisplay(args)
            display.start()
            displayOpt = Some(display)

        case DisplayStop(_) =>
            displayOpt.foreach { display => display.stop() }
            displayOpt = None

        case PlotCommand(spec) =>
            Counter.reset()
            val plotRender: PlotRender = plotProcessor.process(spec)

            plotRender.init()
            try {
                val (specs, dataIter) = plotRender.resultSpecs(batchSize = 500)
                display.submit(specs.toString)
                dataIter.foreach(d => display.submit(d.toString))
            } finally plotRender.close()

        case CommandTimer(command) =>
            val begin: Long = System.nanoTime()
            handleCommand(command)
            val end: Long = System.nanoTime()
            println("Elapsed time: " + (end - begin)/1000000.0 + " ms")

        case Source(filename) =>
            val inps: Iterator[String] =
                scala.io.Source.fromFile(filename).getLines()
            scriptIter(inps).foreach { inp =>
                Counter.reset()
                handleInput(inp)
            }

        case LoggerConfig =>
            val lc: LoggerContext =
                LoggerFactory.getILoggerFactory().asInstanceOf[LoggerContext]
            StatusPrinter.print(lc);

        case OutputFormat(formatOpt) =>
            outputFormatOpt = formatOpt

        case Echo(isEnabled) =>
            isEchoEnabled = isEnabled

        case Reset =>
            Counter.reset()
            processor.dropTemporaryObjects()
    }

    private def handleSqlStatement(stmt: SqlStatement): Unit = stmt match {
        case (s: SqlRelQueryStatement) =>
            processor.handleQueryStatement(s, showResult)
        case (s: SqlUpdateStatement) =>
            processor.handleUpdateStatement(s)
        case (s: SqlAdminStatement) =>
            processor.handleAdminStatement(s)
        case (s: SqlAdminQueryStatement) =>
            showResult(processor.handleAdminQueryStatement(s))
        case other =>
            throw new RuntimeException("Cannot process statement: " + other)
    }

    private def showResult(rs: TableResult): Unit = outputFormatOpt match {
        case Some(csvFormat) =>
            val str: StringBuilder = new StringBuilder()
            val writer: CSVPrinter =
                csvFormat.withHeader(
                    rs.columns.map { col => col.name }: _*
                ).print(str)

            // rows
            rs.rows.foreach { row =>
                writer.printRecord(
                    rs.columns.map { col =>
                        row.getScalValueOpt(col.name, col.sqlType).map {
                            case CharConst(s) => s
                            case DateConst(v) => v.toString
                            case TimeConst(v) => v.toString
                            case TimestampConst(v) => v.toString
                            case other => other.repr
                        } getOrElse ""
                    }: _*
                )
            }

            writer.close()

            println(str.toString)
        case None =>
            val cnames: List[String] = rs.columns.map { col => col.name }
            val rows: Iterator[List[String]] = rs.rows.map { t =>
                cnames.map { cname =>
                    t.getStringOpt(cname).getOrElse("NULL")
                }
            }

            val format: List[String] =
                Format.formatResultSet(cnames, rows.toList)

            format.foreach { s => println(s) }
            println
    }

    private def showResult(
        cnames: List[String],
        rows: List[List[String]]
    ): Unit = {
        Format.formatResultSet(cnames, rows).foreach { s => println(s) }
        println
    }

    private def initProcessor(isInstall: Boolean): Processor = {
        val proc: Processor = Processor()
        try proc.init(isInstall) catch {
            case (e: SQLWarning) =>
                logException(e, logger.warn, Some("SQL Warning"))

            case (e: Throwable) =>
                logException(e, logger.error, Some("Fatal Error"))

                proc.close()
                System.exit(1)
        }

        proc
    }

    private def parseArgs(args: List[String]): (Boolean, List[String]) =
        args match {
            case "-install"::rem => (true, rem)
            case rem => (false, rem)
        }

    def main(args: Array[String]): Unit = {
        val (isInstall, remArgs) = parseArgs(args.toList)

        val proc: Processor = initProcessor(isInstall)
        processorOpt = Some(proc)

        try remArgs.lastOption.map(inp => inp.trim.toUpperCase) match {
            case Some("EXIT" | "QUIT") =>
                remArgs.init.foreach(handleInputInteractive)

            case _ =>
                remArgs.foreach(handleInputInteractive)
                if( remArgs.isEmpty ) println

                val version: String =
                    ScleraConfig.versionOpt.getOrElse("(Unspecified version)")
                println("Welcome to Sclera " + version)
                println

                consoleIter(ScleraConfig.prompt, handleInputInteractive)

                println
        } catch {
            case (e: EndOfFileException) =>
                println("Goodbye!")
            case (e: Throwable) =>
                logException(e, logger.error, Some("Fatal Error"))
                sys.exit(1)
        } finally {
            displayOpt.foreach(_.stop())
            processorOpt.foreach(_.close())
        }

        sys.exit(0)
    }
}
