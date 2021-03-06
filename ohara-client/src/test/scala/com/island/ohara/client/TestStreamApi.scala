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

package com.island.ohara.client
import com.island.ohara.client.configurator.v0.StreamApi
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.VersionUtil
import org.junit.Test
import org.scalatest.Matchers

import scala.util.Random

class TestStreamApi extends SmallTest with Matchers {

  @Test
  def checkVersion(): Unit = {
    StreamApi.STREAMAPP_IMAGE shouldBe s"oharastream/streamapp:${VersionUtil.VERSION}"
  }

  @Test
  def checkAppIdLength(): Unit = {

    val appId = Random.alphanumeric.take(StreamApi.LIMIT_OF_DOCKER_NAME_LENGTH).mkString

    an[IllegalArgumentException] should be thrownBy StreamApi.formatAppId(appId)
  }

}
