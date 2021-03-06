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

import java.io.File

import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterCreationRequest
import com.island.ohara.client.configurator.v0.NodeApi.Node
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterCreationRequest
import com.island.ohara.client.configurator.v0.ZookeeperApi.ZookeeperClusterCreationRequest
import com.island.ohara.client.configurator.v0.{BrokerApi, NodeApi, WorkerApi, ZookeeperApi}
import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.common.util.{CommonUtil, Releasable}
import com.island.ohara.configurator.Configurator
import com.island.ohara.configurator.jar.JarStore
import com.island.ohara.it.IntegrationTest
import com.island.ohara.it.agent.{ClusterNameHolder, CollieTestUtil}
import com.typesafe.scalalogging.Logger
import org.junit.{After, Before, Test}
import org.scalatest.Matchers

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
class TestLoadCustomJarToWorkerCluster extends IntegrationTest with Matchers {

  private[this] val log = Logger(classOf[TestLoadCustomJarToWorkerCluster])

  /**
    * we need to export port to enable remote node download jar from this node
    */
  private[this] val portKey = "ohara.it.port"

  /**
    * we need to export hostname to enable remote node download jar from this node
    */
  private[this] val hostnameKey: String = "ohara.it.hostname"

  private[this] val nodeCache: Seq[Node] = CollieTestUtil.nodeCache()

  private[this] val invalidHostname = "unknown"

  private[this] val invalidPort = 0

  private[this] val publicHostname: String = sys.env.getOrElse(hostnameKey, invalidHostname)

  private[this] val publicPort = sys.env.get(portKey).map(_.toInt).getOrElse(invalidPort)

  private[this] val configurator: Configurator =
    Configurator.builder().advertisedHostname(publicHostname).advertisedPort(publicPort).build()
  private[this] val jarStore: JarStore = configurator.jarStore

  private[this] val zkApi = ZookeeperApi.access().hostname(configurator.hostname).port(configurator.port)

  private[this] val bkApi = BrokerApi.access().hostname(configurator.hostname).port(configurator.port)

  private[this] val wkApi = WorkerApi.access().hostname(configurator.hostname).port(configurator.port)

  private[this] val nameHolder = new ClusterNameHolder(nodeCache)
  @Before
  def setup(): Unit = if (nodeCache.isEmpty || publicPort == invalidPort || publicHostname == invalidHostname)
    skipTest(
      s"${CollieTestUtil.key}, $portKey and $hostnameKey don't exist so all tests in TestLoadCustomJarToWorkerCluster are ignored")
  else {

    val nodeApi = NodeApi.access().hostname(configurator.hostname).port(configurator.port)
    nodeCache.foreach { node =>
      result(
        nodeApi.add(
          NodeApi.NodeCreationRequest(
            name = Some(node.name),
            port = node.port,
            user = node.user,
            password = node.password
          )))
    }
  }

  @Test
  def test(): Unit = {
    val currentPath = new File(".").getCanonicalPath
    // Both jars are pre-generated. see readme in test/resources
    val jars = result(
      Future.traverse(Seq(new File(currentPath, s"src/test/resources/ItConnector.jar"),
                          new File(currentPath, s"src/test/resources/ItConnector2.jar")))(jarStore.add))

    val zkCluster = result(
      zkApi.add(
        ZookeeperClusterCreationRequest(
          name = nameHolder.generateClusterName(),
          imageName = None,
          clientPort = Some(CommonUtil.availablePort()),
          electionPort = Some(CommonUtil.availablePort()),
          peerPort = Some(CommonUtil.availablePort()),
          nodeNames = nodeCache.map(_.name)
        )
      ))
    assertCluster(() => result(zkApi.list()), zkCluster.name)
    log.info(s"zkCluster:$zkCluster")
    val bkCluster = result(
      bkApi.add(
        BrokerClusterCreationRequest(
          name = nameHolder.generateClusterName(),
          imageName = None,
          clientPort = Some(CommonUtil.availablePort()),
          exporterPort = Some(CommonUtil.availablePort()),
          zookeeperClusterName = Some(zkCluster.name),
          nodeNames = nodeCache.map(_.name)
        )
      ))
    assertCluster(() => result(bkApi.list()), bkCluster.name)
    log.info(s"bkCluster:$bkCluster")
    val wkCluster = result(
      wkApi.add(
        WorkerClusterCreationRequest(
          name = nameHolder.generateClusterName(),
          imageName = None,
          clientPort = Some(CommonUtil.availablePort()),
          brokerClusterName = Some(bkCluster.name),
          groupId = None,
          configTopicName = None,
          configTopicReplications = None,
          offsetTopicName = None,
          offsetTopicPartitions = None,
          offsetTopicReplications = None,
          statusTopicName = None,
          statusTopicPartitions = None,
          statusTopicReplications = None,
          jars = jars.map(_.id),
          nodeNames = Seq(nodeCache.head.name)
        )
      ))
    assertCluster(() => result(wkApi.list()), wkCluster.name)
    // add all remaining node to the running worker cluster
    nodeCache.filterNot(n => wkCluster.nodeNames.contains(n.name)).foreach { n =>
      result(wkApi.addNode(wkCluster.name, n.name))
    }
    // make sure all workers have loaded the test-purposed connector.
    result(wkApi.list()).find(_.name == wkCluster.name).get.nodeNames.foreach { name =>
      val workerClient = WorkerClient(s"$name:${wkCluster.clientPort}")
      await(
        () =>
          try result(workerClient.plugins()).exists(_.className == "com.island.ohara.it.ItConnector")
            && result(workerClient.plugins()).exists(_.className == "com.island.ohara.it.ItConnector2")
          catch {
            case _: Throwable => false
        }
      )
    }
  }

  @After
  final def tearDown(): Unit = {
    Releasable.close(configurator)
    Releasable.close(nameHolder)
  }
}
