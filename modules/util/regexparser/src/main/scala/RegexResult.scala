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

import com.scleradb.util.automata.nfa.AnchoredNfa

sealed abstract class RegexResult
case class RegexSuccess(res: AnchoredNfa) extends RegexResult
case class RegexFailure(msg: String) extends RegexResult
