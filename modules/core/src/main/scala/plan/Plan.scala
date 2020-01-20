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

package com.scleradb.plan

private[scleradb]
abstract class Plan {
    def init(): Unit
    def result: PlanResult
    def dispose(): Unit
}

private[scleradb]
object Plan {
    def cleanInit(plans: List[Plan]): Unit =
        plans.foldLeft (List[Plan]()) { case (prevPlans, plan) =>
            try plan.init() catch { case (e: Throwable) =>
                prevPlans.map { prevPlan => prevPlan.dispose() }
                throw e
            }

            plan::prevPlans
        }
}

private[scleradb]
abstract class PlanResult
