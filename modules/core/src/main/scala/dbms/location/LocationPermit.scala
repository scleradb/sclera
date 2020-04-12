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

package com.scleradb.dbms.location

/** Abstract base class for location permits */
sealed abstract class LocationPermit {
    /** String representation of this permit */
    val repr: String

    /** Is writing to the location permitted? */
    def isWritable: Boolean
}

/** Read-only location permit */
object ReadOnlyLocation extends LocationPermit {
    override val repr: String = "READONLY"
    override def isWritable: Boolean = false
}

/** Read-write location permit */
object ReadWriteLocation extends LocationPermit {
    override val repr: String = "READWRITE"
    override def isWritable: Boolean = true
}

/** Companion object - contains constructors for LocationPermit */
object LocationPermit {
    /** Construct LocationPermit object given the string represetation
      *
      * @param str String representation of location permit
      * @return LocationPermit object with the given string representation
      */
    def apply(str: String): LocationPermit = {
        val strUpperCase: String = str.toUpperCase

        if( strUpperCase == ReadOnlyLocation.repr ) ReadOnlyLocation
        else if( strUpperCase == ReadWriteLocation.repr ) ReadWriteLocation
        else throw new IllegalArgumentException(
            "Invalid location type: " + str
        )
    }

    /** Construct LocationPermit object given the string represetation
      *
      * @param strOpt String representation of location permit (optional)
      * @return LocationPermit object with the given string representation
      *         If string representation is not specified, return the Read-Write
      *         location permit.
      */
    def apply(strOpt: Option[String]): LocationPermit = strOpt match {
        case Some(str) => apply(str)
        case None => ReadWriteLocation
    }    
}
