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

package com.scleradb.analytics.sequence.labeler

import scala.language.postfixOps

import com.scleradb.util.automata.datatypes.Label

import com.scleradb.sql.expr.{ColRef, ScalColValue, RelExpr}
import com.scleradb.sql.result.ScalTableRow
import com.scleradb.sql.exec.ScalCastEvaluator

// labels read from an input column
case class ColumnRowLabeler(
    col: ColRef,
    whenThen: List[(List[ScalColValue], List[Label])] = Nil,
    elseOpt: Option[List[Label]] = None, // assigned when nothing matches
    wildcardLabels: List[Label] = Nil // assigned to each row
) extends RowLabeler {
    private val whenThenMap: Map[ScalColValue, List[Label]] = {
        val whenThenPairs: List[(ScalColValue, List[Label])] =
            whenThen.flatMap { case (vs, ls) => vs.map { v => (v, ls) } }
        
        whenThenPairs.groupBy(_._1).view.mapValues { vlss =>
            vlss.map(_._2).flatten
        } toMap
    }

    override def requiredCols: List[ColRef] = List(col)

    override def rowLabels(row: ScalTableRow): List[Label] = {
        val v: ScalColValue = row.getScalExpr(col.name)

        val assigned: List[Label] = whenThenMap.get(v) match {
            case Some(labels) => labels
            case None => elseOpt match {
                case Some(labels) => labels
                case None => ScalCastEvaluator.valueAsStringOpt(v).toList.map {
                    s => Label(s.toUpperCase)
                }
            }
        }
        
       (wildcardLabels ::: assigned) distinct
    }

    override def labeledExpr(input: RelExpr): RelExpr = input

    override def isRedundant(labels: List[Label]): Boolean = false

    override def repr: String = List(
        List("LABEL = " + col.repr),
        whenThen.map { case (ws, ts) =>
            "WHEN (" + ws.map(_.repr).mkString(", ") + ") THEN (" +
            ts.map(label => label.id).mkString(", ") +
            ")"
        },
        elseOpt.toList.map { labels =>
            "DEFAULT (" + labels.map(label => label.id).mkString(", ") + ")"
        }
    ).flatten.mkString(" ")
}
