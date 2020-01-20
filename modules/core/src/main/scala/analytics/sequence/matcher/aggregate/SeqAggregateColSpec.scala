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

package com.scleradb.analytics.sequence.matcher.aggregate

import com.scleradb.util.automata.datatypes.Label
import com.scleradb.util.tools.Counter

import com.scleradb.sql.expr.{ScalExpr, ColRef}
import com.scleradb.sql.datatypes.Column
import com.scleradb.sql.result.TableResult

import com.scleradb.analytics.sequence.labeler.RowLabeler

private[scleradb]
sealed abstract class SeqAggregateColSpec extends java.io.Serializable {
    val resultColRef: ColRef
    val labels: List[Label]
    def aggregate(input: TableResult): SeqAggregate

    def repr: String
}

private[scleradb]
case class SeqColumnSpec(
    override val resultColRef: ColRef,
    colRef: ColRef,
    indexOpt: Option[Int],
    override val labels: List[Label] = Nil
) extends SeqAggregateColSpec {
    override def aggregate(input: TableResult): SeqAggregate = {
        val column: Column = input.columnOpt(colRef.name) getOrElse {
            throw new IllegalArgumentException(
                "Column \"" + colRef.name + "\" not found"
            )
        }

        SeqAggregate(SeqAggregate(indexOpt, column, resultColRef), labels)
    }

    override def repr: String = {
       val labelsStr: String = labels match {
            case Nil => "<HEAD>"
            case List(label) => label.id
            case ls => "LABEL(" + ls.map(l => l.id).mkString(", ") + ")"
        }

        val indexStr: String = indexOpt match {
            case Some(index) => "[" + index + "]"
            case None => ""
        }

        labelsStr + indexStr + "." + colRef.repr + " -> " + resultColRef.repr
    }
}

private[scleradb]
case class SeqFunctionSpec(
    override val resultColRef: ColRef,
    funcName: String,
    params: List[ScalExpr],
    override val labels: List[Label] = Nil
) extends SeqAggregateColSpec {
    override def aggregate(input: TableResult): SeqAggregate =
        SeqAggregate(
            SeqAggregate(funcName, params, input.columns, resultColRef),
            labels
        )

    override def repr: String = {
       val labelsStr: String = labels match {
            case Nil => "<HEAD>"
            case List(label) => label.id
            case ls => "LABEL(" + ls.map(l => l.id).mkString(", ") + ")"
        }

        labelsStr + "." + funcName +
        "(" + params.map(param => param.repr).mkString(", ") + ") -> " +
        resultColRef.repr
    }
}
