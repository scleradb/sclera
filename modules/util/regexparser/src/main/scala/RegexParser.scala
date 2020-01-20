/**
* Sclera - Regular Expression Parser
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

package com.scleradb.util.regexparser

import scala.util.parsing.combinator.syntactical.StdTokenParsers
import scala.util.parsing.combinator.token._

import com.scleradb.util.parsing.CaseInsensitive
import com.scleradb.util.automata.datatypes.Label
import com.scleradb.util.automata.nfa.{Nfa, AnchoredNfa}

object RegexParser extends CaseInsensitive {
    // delimiters
    lexical.delimiters ++= Seq("^", "$", "|", ".", "*", "+", "?", "(", ")")

    // position tracker
    object Position {
        private var pos: Int = 0

        def next: Int = {
            pos += 1
            pos
        }

        def reset(): Unit = { pos = 0 }
    }

    sealed abstract class RegexUnaryOp
    case object RegexKleeneStar extends RegexUnaryOp
    case object RegexKleenePlus extends RegexUnaryOp
    case object RegexOptional extends RegexUnaryOp

    // parser interface - returns the NFA or throws an exception
    def anchoredNfa(r: String): AnchoredNfa =
        parse(r) match {
            case RegexSuccess(nfa) => nfa
            case RegexFailure(msg) => throw new IllegalArgumentException(msg)
        }

    // parser interface
    def parse(r: String): RegexResult = {
        Position.reset()
        anchoredRegex(new lexical.Scanner(r)) match {
            case Success(nfa, _) => RegexSuccess(nfa(r))
            case Failure(msg, _) => RegexFailure(msg)
            case Error(msg, _) => RegexFailure(msg)
        }
    }

    // parser
    def anchoredRegex: Parser[String => AnchoredNfa] =
        opt("^") ~ regex ~ opt("$") ^^ {
            case beginAnchorOpt~nfa~endAnchorOpt =>
                val isAnchoredBegin: Boolean = !beginAnchorOpt.isEmpty
                val isAnchoredEnd: Boolean = !endAnchorOpt.isEmpty
                (toStr: String) =>
                    AnchoredNfa(nfa, isAnchoredBegin, isAnchoredEnd, toStr)
        }

    def regex: Parser[Nfa] = alternate

    def alternate: Parser[Nfa] =
        cascade ~ rep("|" ~> cascade) ^^ {
            case nfa ~ nfas => nfas.foldLeft (nfa) {
                case (prevNfa, nfa) => prevNfa.alternate(nfa)
            }
        }

    def cascade: Parser[Nfa] =
        unary ~ rep(opt(".") ~> unary) ^^ {
            case nfa ~ nfas => nfas.foldLeft (nfa) {
                case (prevNfa, nfa) => prevNfa.cascade(nfa)
            }
        }

    def unary: Parser[Nfa] =
        atom ~ rep(unaryOp) ^^ {
            case atomNfa ~ ops => ops.foldLeft (atomNfa) {
                case (nfa, RegexKleeneStar) => nfa.kleeneStar
                case (nfa, RegexKleenePlus) => nfa.kleenePlus
                case (nfa, RegexOptional) => nfa.optional
            }
        }

    def atom: Parser[Nfa] =
        symbol | "(" ~> regex <~ ")"

    def symbol: Parser[Nfa] =
        (numericLit | stringLit | ident) ^^ {
            id => Nfa(Label(id), Position.next)
        }

    def unaryOp: Parser[RegexUnaryOp] =
        "*" ^^^ RegexKleeneStar |
        "+" ^^^ RegexKleenePlus |
        "?" ^^^ RegexOptional
}
