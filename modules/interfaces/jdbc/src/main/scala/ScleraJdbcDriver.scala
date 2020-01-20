/**
* Sclera - JDBC Driver
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

package com.scleradb.interfaces.jdbc

import java.util.Properties
import java.sql.SQLFeatureNotSupportedException

class ScleraJdbcDriver extends java.sql.Driver {
    override def acceptsURL(url: String): Boolean =
        url.startsWith(ScleraJdbcDriver.baseUrl)

    override def connect(url: String, info: Properties): java.sql.Connection =
        if( acceptsURL(url) ) new Connection(this, url, info)
        else null.asInstanceOf[java.sql.Connection]

    override def getMajorVersion(): Int = ScleraJdbcDriver.majorVersion
    override def getMinorVersion(): Int = ScleraJdbcDriver.minorVersion
    override def jdbcCompliant(): Boolean = false

    override def getParentLogger(): java.util.logging.Logger =
        throw new SQLFeatureNotSupportedException("Logging is not supported")

    override def getPropertyInfo(
        url: String,
        info: Properties
    ): Array[java.sql.DriverPropertyInfo] = Array[java.sql.DriverPropertyInfo]()
}

object ScleraJdbcDriver {
    val majorVersion: Int = 1
    val minorVersion: Int = 0

    val jdbcMajorVersion: Int = 4
    val jdbcMinorVersion: Int = 0

    val productName: String = "scleradb"
    val productMajorVersion: Int = 0
    val productMinorVersion: Int = 1
    
    val baseUrl: String = "jdbc:" + productName
}
