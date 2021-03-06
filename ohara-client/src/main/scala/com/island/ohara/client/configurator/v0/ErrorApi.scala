/*
 * Copyright 2019 is-land
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

package com.island.ohara.client.configurator.v0
import com.island.ohara.client.HttpExecutor
import org.apache.commons.lang3.exception.ExceptionUtils
import spray.json.DefaultJsonProtocol._
import spray.json.RootJsonFormat

object ErrorApi {

  /**
    * Although we have different version of APIs, the error response should be identical so as to simplify the parser...
    * @param code description of code
    * @param message error message
    * @param stack error stack
    */
  final case class Error(code: String, message: String, stack: String) extends HttpExecutor.Error

  def of(e: Throwable): Error =
    Error(e.getClass.getName, if (e.getMessage == null) "unknown" else e.getMessage, ExceptionUtils.getStackTrace(e))

  implicit val ERROR_FORMAT: RootJsonFormat[Error] = jsonFormat3(Error)
}
