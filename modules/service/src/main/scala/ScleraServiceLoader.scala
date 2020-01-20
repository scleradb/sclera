/**
* Sclera - Service Manager
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

package com.scleradb.service

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

/** Utility class to load a service with a given id */
class ScleraServiceLoader[T <: ScleraService](cls: java.lang.Class[T]) {
    /** Service loader */
    private lazy val loader: ServiceLoader[T] = ServiceLoader.load(cls)

    /** Load a service with a given id
      * @param id Identifier of the required service
      * @return Service object with the given id
      */
    def apply(id: String): T = {
        val ssOpt: Option[T] = loader.asScala.find { s => s.accepts(id) }

        ssOpt getOrElse {
            throw new IllegalArgumentException(
                s"""Component "$id" is not installed."""
            )
        }
    }
}
