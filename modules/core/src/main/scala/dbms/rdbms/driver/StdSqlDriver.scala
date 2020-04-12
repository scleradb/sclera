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

package com.scleradb.dbms.rdbms.driver

import java.sql.{PreparedStatement, ResultSet, Blob}
import java.io.ByteArrayInputStream

abstract class StdSqlDriver extends SqlDriver {
    private lazy val byteStore: String =
        location.annotTableName("SCLERA_BYTESTORE")

    override def createByteStore(): Unit = executeUpdate(
        "CREATE TABLE " + byteStore + "(ID VARCHAR(80), PAYLOAD BLOB)"
    )

    override def dropByteStore(): Unit =
        executeUpdate("DROP TABLE " + byteStore + "")

    override def storeBytes(id: String, bytes: Array[Byte]): Unit = {
        val input: ByteArrayInputStream = new ByteArrayInputStream(bytes)

        conn.setAutoCommit(false)

        var sql: String = "INSERT INTO " + byteStore + " VALUES(?, ?)"
        val prepared: PreparedStatement = conn.prepareStatement(sql)

        try {
            prepared.setString(1, id)
            prepared.setBinaryStream(2, input, bytes.size)
            prepared.executeUpdate()
        } catch { case (e: Throwable) =>
            conn.rollback()
            throw e
        } finally {
            input.close()
            prepared.close()
        }

        conn.commit()
    }

    override def readBytes(id: String): Array[Byte] = {
        var sql: String = "SELECT PAYLOAD FROM " + byteStore + " WHERE ID = ?"
        val prepared: PreparedStatement = conn.prepareStatement(sql)

        try {
            prepared.setString(1, id)
            val rs: ResultSet = prepared.executeQuery()
            try {
                if( rs.next() ) {
                    val blob: Blob = rs.getBlob(1)

                    try blob.getBytes(1, blob.length().toInt)
                    finally blob.free()
                } else {
                    throw new IllegalArgumentException(
                        "Cannot read the bytes for \"" + id + "\""
                    )
                }
            } finally rs.close()
        } finally prepared.close()
    }

    override def removeBytes(id: String): Unit = {
        var sql: String = "DELETE FROM " + byteStore + " WHERE ID = ?"
        val prepared: PreparedStatement = conn.prepareStatement(sql)
        try {
            prepared.setString(1, id)
            prepared.executeUpdate()
        } finally {
            prepared.close()
        }
    }
}
