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

/** Base class for NFA */
abstract class NfaBase extends java.io.Serializable {
    /** Label associated with a state */
    val stateLabel: Map[State, Label]

    /** Transition map -- gives the next states associated with each state */
    val next: Map[State, List[State]]

    /** States */
    def states: List[State] = stateLabel.keys.toList

    /** Labels */
    def labels: List[Label] = stateLabel.values.toList.distinct

    /** Previous state (inverse of next) */
    lazy val prev: Map[State, List[State]] = Map() ++
        states.map { x => (x -> states.filter { y => next(y) contains x }) }

    /** States associated with a label (inverse of stateLabel) */
    lazy val labelStates: Map[Label, List[State]] = Map() ++
        labels.map { x => (x -> states.filter { y => stateLabel(y) == x }) }

    /** States associated with a list of labels */
    def labelStates(ls: List[Label]): List[State] =
        ls.flatMap { label => labelStates.get(label) getOrElse Nil }
}
