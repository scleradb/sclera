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

import java.sql.{SQLFeatureNotSupportedException, SQLDataException}

class ParameterMetaData(nParams: Int) extends java.sql.ParameterMetaData {
    override def getParameterClassName(param: Int): String =
        throw new SQLFeatureNotSupportedException(
            "Function \"getParameterClassName\" is not supported"
        )

    override def getParameterCount(): Int = nParams

    override def getParameterMode(param: Int): Int =
        java.sql.ParameterMetaData.parameterModeUnknown

    override def getParameterType(param: Int): Int =
        throw new SQLFeatureNotSupportedException(
            "Function \"getParameterType\" is not supported"
        )

    override def getParameterTypeName(param: Int): String =
        throw new SQLFeatureNotSupportedException(
            "Function \"getParameterTypeName\" is not supported"
        )

    override def getPrecision(param: Int): Int =
        throw new SQLFeatureNotSupportedException(
            "Function \"getPrecision\" is not supported"
        )

    override def getScale(param: Int): Int =
        throw new SQLFeatureNotSupportedException(
            "Function \"getScale\" is not supported"
        )

    override def isNullable(param: Int): Int =
        throw new SQLFeatureNotSupportedException(
            "Function \"isNullable\" is not supported"
        )

    override def isSigned(param: Int): Boolean =
        throw new SQLFeatureNotSupportedException(
            "Function \"isSigned\" is not supported"
        )

    override def isWrapperFor(iface: java.lang.Class[_]) = false
    override def unwrap[T](iface: java.lang.Class[T]): T =
        throw new SQLDataException("Not a wrapper")
}


