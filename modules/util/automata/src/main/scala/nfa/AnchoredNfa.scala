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

/** Anchored Nfa */
class AnchoredNfa(
    val nfa: Nfa,
    val isAnchoredBegin: Boolean,
    val isAnchoredEnd: Boolean,
    override val toString: String
) extends NfaBase {
    /** Start state */
    val startState: State = State(0)

    /** Finish states */
    val finishStates: List[State] =
        if( nfa.isOptional ) startState::nfa.last else nfa.last

    override val stateLabel: Map[State, Label] =
        nfa.stateLabel + (startState -> Label("$START$"))

    override val next: Map[State, List[State]] =
        nfa.next + (startState -> nfa.first)

    /** pretty printing */
    def repr: List[String] = List(
        "[StateLabel] " + stateLabel,
        "[Start] " + startState,
        "[Finish] " + finishStates,
        "[Next] " + next
    )
}

object AnchoredNfa {
    def apply(
        nfa: Nfa,
        isAnchoredBegin: Boolean = false,
        isAnchoredEnd: Boolean = false,
        toStr: String
    ): AnchoredNfa = new AnchoredNfa(nfa, isAnchoredBegin, isAnchoredEnd, toStr)

    def kleeneStar(
        s: Label,
        isAnchoredBegin: Boolean = false,
        isAnchoredEnd: Boolean = false
    ): AnchoredNfa = apply(
        Nfa.kleeneStar(s), isAnchoredBegin, isAnchoredEnd, "(" + s.id + ")*"
    )

    def kleenePlus(
        s: Label,
        isAnchoredBegin: Boolean = false,
        isAnchoredEnd: Boolean = false
    ): AnchoredNfa = apply(
        Nfa.kleenePlus(s), isAnchoredBegin, isAnchoredEnd, "(" + s.id + ")+"
    )
}
