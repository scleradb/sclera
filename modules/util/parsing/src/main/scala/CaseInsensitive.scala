/**
* Sclera - Parsing
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

package com.scleradb.util.parsing

import scala.language.implicitConversions

import scala.util.parsing.input.CharArrayReader.EofCh

import scala.util.parsing.combinator.syntactical.StdTokenParsers
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.token._

/** Case-insensitive identifiers/keywords, strings with escape sequence */
trait CaseInsensitive extends StdTokenParsers {
    type Tokens = StdTokens

    override val lexical: StdLexical = new StdLexical {
        override def processIdent(s: String) = super.processIdent(s.toUpperCase)

        override def token: Parser[Token] =
            'E' ~> '\'' ~>
            rep(escSeq | chrExcept('\'', '\n', EofCh)) <~ '\'' ^^ {
                chars => StringLit(chars mkString "")
            } |
            '`' ~> rep1(chrExcept('`', '\n', EofCh)) <~ '`' ^^ {
                chars => Identifier((chars mkString "").toUpperCase)
            } |
            super.token

        private def escSeq: Parser[Char] =
            '\'' ~ '\'' ^^^ '\'' |
            '\\' ~ '\'' ^^^ '\'' |
            '\\' ~ '\\' ^^^ '\\' |
            '\\' ~ 'b'  ^^^ '\b' |
            '\\' ~ 'f'  ^^^ '\f' |
            '\\' ~ 'n'  ^^^ '\n' |
            '\\' ~ 'r'  ^^^ '\r' |
            '\\' ~ 't'  ^^^ '\t' |
            '\\' ~> octalBlock |
            '\\' ~> 'x' ~> hexBlock |
            '\\' ~> 'u' ~> unicodeBlock16 |
            '\\' ~> 'U' ~> unicodeBlock32 |
            '\\' ~> chrExcept(EofCh::("'\\bfnrtuUx01234567").toList :_*)

        private def octalBlock: Parser[Char] =
            octDigit ~ opt(octDigit ~ opt(octDigit)) ^^ {
                case a~None => Integer.parseInt(a.toString, 8).toChar
                case a~Some(b~None) =>
                    Integer.parseInt(List(a, b) mkString "", 8).toChar
                case a~Some(b~Some(c)) =>
                    Integer.parseInt(List(a, b, c) mkString "", 8).toChar
            }

        private def octDigit: Parser[Char] =
            elem("octal digit", "01234567".contains(_))

        private def unicodeBlock16: Parser[Char] =
            repN(4, hexDigit) ^^ {
                hex => Integer.parseInt(hex mkString "", 16).toChar
            }

        private def unicodeBlock32: Parser[Char] =
            repN(8, hexDigit) ^^ {
                hex => Integer.parseInt(hex mkString "", 16).toChar
            }

        private def hexBlock: Parser[Char] =
            hexDigit ~ opt(hexDigit) ^^ {
                case a~None => Integer.parseInt(a.toString, 16).toChar
                case a~Some(b) =>
                    Integer.parseInt(List(a, b) mkString "", 16).toChar
            }

        private def hexDigit: Parser[Char] =
            elem("hex digit", "0123456789abcdefABCDEF".contains(_))
    }

    override implicit def keyword(chars: String): Parser[String] =
        if( lexical.reserved.contains(chars) ||
            lexical.delimiters.contains(chars) ) super.keyword(chars)
        else failure("Token not recognized: \"" + chars + "\"")
}
