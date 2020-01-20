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

package com.scleradb.sql.datatypes

import com.scleradb.sql.types.SqlType

/** Table column
  * @param name Name of the column
  * @param sqlType SQL type of the column
  * @param familyOpt The family of the column
  *                  (optional, defined if supported by the underlying platform)
  */
case class Column(
    name: String, sqlType: SqlType, familyOpt: Option[String] = None
)
