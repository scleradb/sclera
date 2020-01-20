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

package com.scleradb.dbms.driver

import com.scleradb.objects.DbObjectDuration
import com.scleradb.sql.objects.Table

/** Metadata driver */
abstract class StatementMetadataDriver {
    /** Table metadata
      * @param tableName Name of the table for which the metadata is requested
      * @return Table object containing the metadata, and the table's duration
      */
    def table(tableName: String): (Table, DbObjectDuration)

    /** Table metadata
      * @param tableName Name of the table for which the metadata is requested
      * @param tableType Table.BaseTable or Table.BaseView
      * @return Table object containing the metadata
      */
    def table(tableName: String, tableType: Table.BaseType): Table

    /** Metadata of all the tables at this location
      * @return For each table, the Table object containing the metadata,
      *         and the table's duration
      */
    def tables: List[(Table, DbObjectDuration)]

    /** Close the metadata driver */
    def close(): Unit
}
