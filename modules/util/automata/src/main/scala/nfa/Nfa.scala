/**
* Sclera - Automata
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

package com.scleradb.util.automata.nfa

import com.scleradb.util.automata.datatypes.{State, Label}

/** Glushkov automaton */
class Nfa(
    override val stateLabel: Map[State, Label],
    val first: List[State],
    val last: List[State],
    override val next: Map[State, List[State]],
    val isOptional: Boolean
) extends NfaBase {
    /** Alternate with another NFA */
    def alternate(alt: Nfa): Nfa = {
        val nfaStateLabel = stateLabel ++ alt.stateLabel
        val nfaFirst = first ::: alt.first
        val nfaLast = last ::: alt.last
        val nfaNext = next ++ alt.next
        val nfaIsOptional = isOptional || alt.isOptional
        new Nfa(nfaStateLabel, nfaFirst, nfaLast, nfaNext, nfaIsOptional)
    }

    /** Cascade with another NFA */
    def cascade(follow: Nfa): Nfa = {
        val nfaStateLabel = stateLabel ++ follow.stateLabel
        val nfaFirst =
            if( isOptional ) first ::: follow.first else first
        val nfaLast =
            if( follow.isOptional ) last ::: follow.last else follow.last
        val nfaNext = follow.next ++ last.foldLeft (next) {
            (map, x) => map + (x -> (map(x) ::: follow.first))
        }
        val nfaIsOptional = isOptional && follow.isOptional
        new Nfa(nfaStateLabel, nfaFirst, nfaLast, nfaNext, nfaIsOptional)
    }

    /** Add the 'Optional' (?) operator */
    def optional = {
        val nfaIsOptional = true
        new Nfa(stateLabel, first, last, next, nfaIsOptional)
    }

    /** Add the 'Kleene Plus' (+) operator */
    def kleenePlus: Nfa = {
        val nfaNext = last.foldLeft (next) {
            (map, x) => map + (x -> (map(x) ::: first).distinct)
        }
        new Nfa(stateLabel, first, last, nfaNext, isOptional)
    }

    /** Add the 'Kleene Star' (*) operator */
    def kleeneStar: Nfa = kleenePlus.optional

    /** Reverse the NFA */
    def reverse: Nfa = {
        val nfaFirst = last
        val nfaLast = first
        val nfaNext = prev
        new Nfa(stateLabel, nfaFirst, nfaLast, nfaNext, isOptional)
    }

    override def toString: String = List(
        "[StateLabel] " + stateLabel,
        "[First] " + first,
        "[Last] " + last,
        "[Next] " + next,
        "[IsOptional] " + isOptional
    ).mkString(" ")
}

object Nfa {
    def apply (
        stateLabel: Map[State, Label],
        first: List[State],
        last: List[State],
        next: Map[State, List[State]],
        isOptional: Boolean
    ): Nfa = new Nfa(stateLabel, first, last, next, isOptional)

    // construct NFA for a single symbol at a unique position
    def apply(symbol: Label, pos: Int): Nfa = {
        val state = State(pos)

        val nfaStateLabel = Map(state -> symbol)
        val nfaFirst, nfaLast = List(state)
        val nfaNext = Map(state -> Nil)
        val nfaIsOptional = false
        apply(nfaStateLabel, nfaFirst, nfaLast, nfaNext, nfaIsOptional)
    }

    def kleeneStar(s: Label): Nfa = apply(s, 1).kleeneStar

    def kleenePlus(s: Label): Nfa = apply(s, 1).kleenePlus
}
