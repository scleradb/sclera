/**
* Sclera - Display
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

package com.scleradb.interfaces.display.service

import com.scleradb.service.{ScleraService, ScleraServiceLoader}
import com.scleradb.config.ScleraConfig

import com.scleradb.interfaces.display.Display

/** Display service */
abstract class DisplayService extends ScleraService {
    /** Creates a Display object given the generic parameters
      * @param params Generic parameters
      */
    def createDisplay(params: List[String]): Display
}

private[scleradb]
object DisplayService extends ScleraServiceLoader(classOf[DisplayService]) {
    def apply(idOpt: Option[String] = None): DisplayService =
        apply(idOpt getOrElse ScleraConfig.defaultDisplayService)
}
