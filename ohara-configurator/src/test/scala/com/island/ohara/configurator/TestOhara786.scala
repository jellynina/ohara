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

package com.island.ohara.configurator

import com.island.ohara.client.configurator.v0.TopicApi
import com.island.ohara.client.configurator.v0.TopicApi.TopicCreationRequest
import com.island.ohara.kafka.BrokerClient
import com.island.ohara.testing.WithBrokerWorker
import org.junit.Test
import org.scalatest.Matchers

import scala.concurrent.Await
import scala.concurrent.duration._
class TestOhara786 extends WithBrokerWorker with Matchers {

  private[this] val configurator =
    Configurator.builder().fake(testUtil.brokersConnProps(), testUtil().workersConnProps()).build()

  @Test
  def deleteAnTopicRemovedFromKafka(): Unit = {
    val topicName = methodName

    val topic = Await.result(
      TopicApi
        .access()
        .hostname(configurator.hostname)
        .port(configurator.port)
        .add(
          TopicCreationRequest(name = Some(topicName),
                               brokerClusterName = None,
                               numberOfPartitions = None,
                               numberOfReplications = None)),
      10 seconds
    )
    val brokerClient = BrokerClient.of(testUtil().brokersConnProps())
    try {
      brokerClient.deleteTopic(topic.id)
      // the topic is removed but we don't throw exception.
      Await
        .result(TopicApi.access().hostname(configurator.hostname).port(configurator.port).delete(topic.id), 10 seconds)
    } finally brokerClient.close()
  }
}
