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

package com.island.ohara.configurator.route

import com.island.ohara.client.configurator.v0.ConnectorApi.ConnectorConfigurationRequest
import com.island.ohara.client.configurator.v0.HadoopApi.HdfsInfoRequest
import com.island.ohara.client.configurator.v0.NodeApi.NodeCreationRequest
import com.island.ohara.client.configurator.v0.PipelineApi.{Pipeline, PipelineCreationRequest}
import com.island.ohara.client.configurator.v0.TopicApi.TopicCreationRequest
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterCreationRequest
import com.island.ohara.client.configurator.v0._
import com.island.ohara.common.rule.SmallTest
import com.island.ohara.common.util.Releasable
import com.island.ohara.configurator.Configurator
import org.junit.{After, Test}
import org.scalatest.Matchers

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class TestPipelineRoute extends SmallTest with Matchers {
  private[this] val configurator = Configurator.builder().fake(1, 1).build()

  private[this] def result[T](f: Future[T]): T = Await.result(f, 10 seconds)

  private[this] val pipelineAccess = PipelineApi.access().hostname(configurator.hostname).port(configurator.port)

  @Test
  def testMultiWorkerCluster(): Unit = {

    val pipeline0 = result(
      pipelineAccess.add(
        PipelineCreationRequest(
          name = methodName(),
          rules = Map.empty
        ))
    )
    pipeline0.workerClusterName shouldBe result(
      configurator.clusterCollie.workerCollie().cluster(pipeline0.workerClusterName))._1.name

    // add node
    result(
      NodeApi
        .access()
        .hostname(configurator.hostname)
        .port(configurator.port)
        .add(
          NodeCreationRequest(
            name = Some(methodName()),
            port = 22,
            user = methodName(),
            password = methodName()
          ))
    )
    val wkCluster = result(
      WorkerApi
        .access()
        .hostname(configurator.hostname)
        .port(configurator.port)
        .add(
          WorkerClusterCreationRequest(
            name = methodName(),
            imageName = None,
            brokerClusterName = None,
            clientPort = None,
            nodeNames = Seq(methodName()),
            jars = Seq.empty
          ))
    )

    val pipeline1 = result(
      pipelineAccess.add(wkCluster.name,
                         PipelineCreationRequest(
                           name = methodName(),
                           rules = Map.empty
                         ))
    )

    pipeline1.workerClusterName shouldBe wkCluster.name

    val pipelines = result(pipelineAccess.list())
    pipelines.size shouldBe 2
    pipelines.find(_.id == pipeline0.id).get.workerClusterName shouldBe pipeline0.workerClusterName
    pipelines.find(_.id == pipeline1.id).get.workerClusterName shouldBe pipeline1.workerClusterName
  }

  @Test
  def testUnmatchedId(): Unit = {
    // test invalid request: nonexistent uuid
    val invalidRequest = PipelineCreationRequest(methodName, Map("invalid" -> "invalid"))
    val pipeline = result(pipelineAccess.add(invalidRequest))
    pipeline.rules.size shouldBe 1
    pipeline.rules(PipelineApi.UNKNOWN) shouldBe PipelineApi.UNKNOWN
  }

  @Test
  def testNormalCase(): Unit = {
    val connector = Await.result(
      ConnectorApi
        .access()
        .hostname(configurator.hostname)
        .port(configurator.port)
        .add(
          ConnectorConfigurationRequest(
            name = methodName(),
            className = methodName(),
            topics = Seq.empty,
            numberOfTasks = 1,
            schema = Seq.empty,
            configs = Map.empty
          )),
      10 seconds
    )

    val topic = Await.result(
      TopicApi
        .access()
        .hostname(configurator.hostname)
        .port(configurator.port)
        .add(
          TopicCreationRequest(
            name = methodName(),
            numberOfPartitions = 1,
            numberOfReplications = 1
          )),
      10 seconds
    )

    val pipeline = Await.result(
      pipelineAccess.add(
        PipelineCreationRequest(
          name = methodName(),
          rules = Map(connector.id -> topic.id)
        )),
      10 seconds
    )

    pipeline.rules.size shouldBe 1
    pipeline.rules(connector.id) shouldBe topic.id

    // remove connector
    Await.result(ConnectorApi.access().hostname(configurator.hostname).port(configurator.port).delete(connector.id),
                 10 seconds)

    val pipeline2 = Await.result(pipelineAccess.get(pipeline.id), 10 seconds)

    pipeline2.rules.size shouldBe 1
    pipeline2.rules(PipelineApi.UNKNOWN) shouldBe topic.id

    val pipelines = Await.result(pipelineAccess.list(), 10 seconds)

    pipelines.size shouldBe 1
    pipelines.head.rules.size shouldBe 1
    pipelines.head.rules(PipelineApi.UNKNOWN) shouldBe topic.id
  }

  @Test
  def testPipeline(): Unit = {
    def compareRequestAndResponse(request: PipelineCreationRequest, response: Pipeline): Pipeline = {
      request.name shouldBe response.name
      request.rules shouldBe response.rules
      response
    }

    def compare2Response(lhs: Pipeline, rhs: Pipeline): Unit = {
      lhs.id shouldBe rhs.id
      lhs.name shouldBe rhs.name
      lhs.rules shouldBe rhs.rules
      lhs.objects shouldBe rhs.objects
    }

    // test add
    val topicAccess = TopicApi.access().hostname(configurator.hostname).port(configurator.port)
    val uuid_0 = result(topicAccess.add(TopicCreationRequest(methodName(), 1, 1))).id
    val uuid_1 = result(topicAccess.add(TopicCreationRequest(methodName(), 1, 1))).id
    val uuid_2 = result(topicAccess.add(TopicCreationRequest(methodName(), 1, 1))).id

    result(pipelineAccess.list()).size shouldBe 0

    val request = PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_1))
    val response = compareRequestAndResponse(request, result(pipelineAccess.add(request)))

    // test get
    compare2Response(response, result(pipelineAccess.get(response.id)))

    // test update
    val anotherRequest = PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_2))
    val newResponse =
      compareRequestAndResponse(anotherRequest, result(pipelineAccess.update(response.id, anotherRequest)))

    // topics should have no state
    newResponse.objects.foreach(_.state shouldBe None)

    // test get
    compare2Response(newResponse, result(pipelineAccess.get(newResponse.id)))

    // test delete
    result(pipelineAccess.list()).size shouldBe 1
    result(pipelineAccess.delete(response.id)) shouldBe newResponse
    result(pipelineAccess.list()).size shouldBe 0

    // test nonexistent data
    an[IllegalArgumentException] should be thrownBy result(pipelineAccess.get("asdasdsad"))
    an[IllegalArgumentException] should be thrownBy result(pipelineAccess.update("asdasdsad", anotherRequest))
  }

  @Test
  def testBindInvalidObjects2Pipeline(): Unit = {
    val topicAccess = TopicApi.access().hostname(configurator.hostname).port(configurator.port)
    val hdfsAccess = HadoopApi.access().hostname(configurator.hostname).port(configurator.port)
    val uuid_0 = result(topicAccess.add(TopicCreationRequest(methodName(), 1, 1))).id
    val uuid_1 = result(hdfsAccess.add(HdfsInfoRequest(methodName, "file:///"))).id
    val uuid_2 = result(hdfsAccess.add(HdfsInfoRequest(methodName, "file:///"))).id
    val uuid_3 = result(topicAccess.add(TopicCreationRequest(methodName(), 1, 1))).id
    result(topicAccess.list()).size shouldBe 2
    result(hdfsAccess.list()).size shouldBe 2

    // uuid_0 -> uuid_0: self-bound
    an[IllegalArgumentException] should be thrownBy result(
      pipelineAccess.add(PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_0))))
    // uuid_1 can't be applied to pipeline
    an[IllegalArgumentException] should be thrownBy result(
      pipelineAccess.add(PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_1))))
    // uuid_2 can't be applied to pipeline
    an[IllegalArgumentException] should be thrownBy result(
      pipelineAccess.add(PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_2))))

    val res = result(pipelineAccess.add(PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_3))))
    // uuid_0 -> uuid_0: self-bound
    an[IllegalArgumentException] should be thrownBy result(
      pipelineAccess.update(res.id, PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_0))))
    // uuid_1 can't be applied to pipeline
    an[IllegalArgumentException] should be thrownBy result(
      pipelineAccess.update(res.id, PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_1))))
    // uuid_2 can't be applied to pipeline
    an[IllegalArgumentException] should be thrownBy result(
      pipelineAccess.update(res.id, PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_2))))

    // good case
    result(pipelineAccess.update(res.id, PipelineCreationRequest(methodName, Map(uuid_0 -> uuid_3)))).name shouldBe methodName
  }
  @After
  def tearDown(): Unit = Releasable.close(configurator)
}