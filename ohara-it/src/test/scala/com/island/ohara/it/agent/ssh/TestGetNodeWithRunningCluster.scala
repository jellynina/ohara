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

package com.island.ohara.it.agent.ssh

import com.island.ohara.agent._
import com.island.ohara.client.configurator.v0.NodeApi.{Node, NodeCreationRequest}
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterCreationRequest
import com.island.ohara.client.configurator.v0.{NodeApi, ZookeeperApi}
import com.island.ohara.common.util.{CommonUtil, Releasable}
import com.island.ohara.configurator.Configurator
import com.island.ohara.it.IntegrationTest
import com.island.ohara.it.agent.CollieTestUtil
import org.junit.{After, Before, Test}
import org.scalatest.Matchers
class TestGetNodeWithRunningCluster extends IntegrationTest with Matchers {

  private[this] val nodeCache: Seq[Node] = CollieTestUtil.nodeCache()

  private[this] val configurator: Configurator = Configurator.builder().build()

  @Before
  def setup(): Unit = if (nodeCache.isEmpty) skipTest(s"${CollieTestUtil.key} is required")
  else {
    nodeCache.foreach { node =>
      val dockerClient =
        DockerClient.builder().hostname(node.name).port(node.port).user(node.user).password(node.password).build()
      try {
        withClue(s"failed to find ${ZookeeperApi.IMAGE_NAME_DEFAULT}")(
          dockerClient.imageNames().contains(ZookeeperApi.IMAGE_NAME_DEFAULT) shouldBe true)
      } finally dockerClient.close()
    }
    nodeCache.foreach { node =>
      result(
        NodeApi
          .access()
          .hostname(configurator.hostname)
          .port(configurator.port)
          .add(NodeCreationRequest(Some(node.name), node.port, node.user, node.password)))
    }
  }

  @Test
  def test(): Unit = {
    val cluster = result(
      ZookeeperApi
        .access()
        .hostname(configurator.hostname)
        .port(configurator.port)
        .add(ZookeeperClusterCreationRequest(
          name = CommonUtil.randomString(10),
          imageName = None,
          clientPort = Some(CommonUtil.availablePort()),
          electionPort = Some(CommonUtil.availablePort()),
          peerPort = Some(CommonUtil.availablePort()),
          nodeNames = nodeCache.map(_.name)
        )))

    try {
      assertCluster(() => result(ZookeeperApi.access().hostname(configurator.hostname).port(configurator.port).list()),
                    cluster.name)
      val nodes = result(NodeApi.access().hostname(configurator.hostname).port(configurator.port).list())
      nodes.isEmpty shouldBe false
      nodes.foreach { node =>
        node.services.isEmpty shouldBe false
        withClue(s"${node.services}")(node.services.map(_.clusterNames.size).sum > 0 shouldBe true)
      }
    } finally result(ZookeeperApi.access().hostname(configurator.hostname).port(configurator.port).delete(cluster.name))
  }

  @After
  final def tearDown(): Unit = Releasable.close(configurator)
}
