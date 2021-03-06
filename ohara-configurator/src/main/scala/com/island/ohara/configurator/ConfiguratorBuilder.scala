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

import akka.http.scaladsl.server
import com.island.ohara.agent._
import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import com.island.ohara.client.configurator.v0.NodeApi.{Node, NodeService}
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import com.island.ohara.client.configurator.v0.{InfoApi, NodeApi}
import com.island.ohara.client.kafka.WorkerClient
import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.data.Serializer
import com.island.ohara.common.util.CommonUtil
import com.island.ohara.configurator.Configurator.Store
import com.island.ohara.configurator.fake._

import scala.concurrent.duration._
import scala.concurrent.{Await, Future}

class ConfiguratorBuilder {
  private[this] var advertisedHostname: Option[String] = None
  private[this] var advertisedPort: Option[Int] = None
  private[this] val store: Store = new Store(
    com.island.ohara.configurator.store.Store.inMemory(Serializer.STRING, Configurator.DATA_SERIALIZER))
  private[this] var initializationTimeout: Option[Duration] = Some(10 seconds)
  private[this] var terminationTimeout: Option[Duration] = Some(10 seconds)
  private[this] var extraRoute: Option[server.Route] = None
  private[this] var clusterCollie: Option[ClusterCollie] = None

  @Optional("default is none")
  def extraRoute(extraRoute: server.Route): ConfiguratorBuilder = {
    this.extraRoute = Some(extraRoute)
    this
  }

  /**
    * set advertised hostname which will be exposed by configurator.
    *
    * @param hostname used to build the rest server
    * @return this builder
    */
  @Optional("default is localhost")
  def advertisedHostname(hostname: String): ConfiguratorBuilder = {
    this.advertisedHostname = Some(hostname)
    this
  }

  /**
    * set advertised port which will be exposed by configurator.
    * Noted: configurator is bound on this port also.
    * @param port used to build the rest server
    * @return this builder
    */
  @Optional("default is random port")
  def advertisedPort(port: Int): ConfiguratorBuilder = {
    this.advertisedPort = Some(port)
    this
  }

  @Optional("default is 10 seconds")
  def terminationTimeout(terminationTimeout: Duration): ConfiguratorBuilder = {
    this.terminationTimeout = Some(terminationTimeout)
    this
  }

  @Optional("default is 10 seconds")
  def initializationTimeout(initializationTimeout: Duration): ConfiguratorBuilder = {
    this.initializationTimeout = Some(initializationTimeout)
    this
  }

  /**
    * set all client to fake mode with a pre-created broker cluster and worker cluster.
    *
    * @return this builder
    */
  def fake(): ConfiguratorBuilder = fake(1, 1)

  /**
    * set all client to fake mode but broker client and worker client is true that they are connecting to embedded cluster.
    *
    * @return this builder
    */
  def fake(bkConnectionProps: String, wkConnectionProps: String): ConfiguratorBuilder = {
    val embeddedBkName = "embedded_broker_cluster"
    val embeddedWkName = "embedded_worker_cluster"
    // we fake nodes for embedded bk and wk
    def nodes(s: String): Seq[String] = s.split(",").map(_.split(":").head)
    (nodes(bkConnectionProps) ++ nodes(wkConnectionProps))
    // DON'T add duplicate nodes!!!
      .toSet[String]
      .map { nodeName =>
        FakeNode(
          name = nodeName,
          services = (if (bkConnectionProps.contains(nodeName))
                        Seq(NodeService(NodeApi.BROKER_SERVICE_NAME, Seq(embeddedBkName)))
                      else Seq.empty) ++ (if (wkConnectionProps.contains(nodeName))
                                            Seq(NodeService(NodeApi.WORKER_SERVICE_NAME, Seq(embeddedWkName)))
                                          else Seq.empty),
          port = -1,
          user = "fake user",
          password = "fake password",
          lastModified = CommonUtil.current()
        )
      }
      .foreach(store.add)
    val collie = new FakeClusterCollie(store, bkConnectionProps, wkConnectionProps)
    val bkCluster = {
      val pair = bkConnectionProps.split(",")
      val host = pair.map(_.split(":").head).head
      val port = pair.map(_.split(":").last).head.toInt
      BrokerClusterInfo(
        name = embeddedBkName,
        imageName = "None",
        zookeeperClusterName = "None",
        exporterPort = -1,
        clientPort = port,
        nodeNames = Seq(host)
      )
    }
    val connectorVersions =
      Await.result(WorkerClient(wkConnectionProps).plugins(), 10 seconds).map(InfoApi.toConnectorVersion)
    val wkCluster = {
      val pair = wkConnectionProps.split(",")
      val host = pair.map(_.split(":").head).head
      val port = pair.map(_.split(":").last).head.toInt
      WorkerClusterInfo(
        name = embeddedWkName,
        imageName = "None",
        brokerClusterName = bkCluster.name,
        clientPort = port,
        groupId = "None",
        statusTopicName = "None",
        statusTopicPartitions = 1,
        statusTopicReplications = 1.asInstanceOf[Short],
        configTopicName = "None",
        configTopicPartitions = 1,
        configTopicReplications = 1.asInstanceOf[Short],
        offsetTopicName = "None",
        offsetTopicPartitions = 1,
        offsetTopicReplications = 1.asInstanceOf[Short],
        jarNames = Seq.empty,
        sources = connectorVersions.filter(_.typeName.toLowerCase == "source"),
        sinks = connectorVersions.filter(_.typeName.toLowerCase == "sink"),
        nodeNames = Seq(host)
      )
    }
    collie.brokerCollie().addCluster(bkCluster)
    collie.workerCollie().addCluster(wkCluster)
    clusterCollie(collie)
  }

  /**
    * Create a fake collie with specified number of broker/worker cluster.
    * @param numberOfBrokerCluster number of broker cluster
    * @param numberOfWorkerCluster number of worker cluster
    * @return this builder
    */
  def fake(numberOfBrokerCluster: Int, numberOfWorkerCluster: Int): ConfiguratorBuilder = {
    if (numberOfBrokerCluster < 0)
      throw new IllegalArgumentException(s"numberOfBrokerCluster:$numberOfBrokerCluster should be positive")
    if (numberOfWorkerCluster < 0)
      throw new IllegalArgumentException(s"numberOfWorkerCluster:$numberOfWorkerCluster should be positive")
    if (numberOfBrokerCluster <= 0 && numberOfWorkerCluster > 0)
      throw new IllegalArgumentException(s"you must initialize bk cluster before you initialize wk cluster")
    val collie = new FakeClusterCollie(store)

    val zkClusters = (0 until numberOfBrokerCluster).map { index =>
      collie
        .zookeeperCollie()
        .addCluster(FakeZookeeperClusterInfo(
          name = s"fakeZkCluster$index",
          imageName = s"fakeImage$index",
          // Assigning a negative value can make test fail quickly.
          clientPort = -1,
          electionPort = -1,
          peerPort = -1,
          nodeNames = (0 to 2).map(_ => CommonUtil.randomString(5))
        ))
    }

    // add broker cluster
    val bkClusters = zkClusters.zipWithIndex.map {
      case (zkCluster, index) =>
        collie
          .brokerCollie()
          .addCluster(FakeBrokerClusterInfo(
            name = s"fakeBkCluster$index",
            imageName = s"fakeImage$index",
            zookeeperClusterName = zkCluster.name,
            // Assigning a negative value can make test fail quickly.
            clientPort = -1,
            exporterPort = -1,
            nodeNames = zkCluster.nodeNames
          ))
    }

    // we don't need to collect wk clusters
    (0 until numberOfWorkerCluster).foreach { index =>
      val bkCluster = bkClusters((Math.random() % bkClusters.size).asInstanceOf[Int])
      collie
        .workerCollie()
        .addCluster(FakeWorkerClusterInfo(
          name = s"fakeWkCluster$index",
          imageName = s"fakeImage$index",
          brokerClusterName = bkCluster.name,
          // Assigning a negative value can make test fail quickly.
          clientPort = -1,
          groupId = s"groupId$index",
          statusTopicName = s"statusTopicName$index",
          statusTopicPartitions = 1,
          statusTopicReplications = 1.asInstanceOf[Short],
          configTopicName = s"configTopicName$index",
          configTopicPartitions = 1,
          configTopicReplications = 1.asInstanceOf[Short],
          offsetTopicName = s"offsetTopicName$index",
          offsetTopicPartitions = 1,
          offsetTopicReplications = 1.asInstanceOf[Short],
          jarNames = Seq.empty,
          sources = Seq.empty,
          sinks = Seq.empty,
          nodeNames = bkCluster.nodeNames
        ))

    }
    // fake nodes
    zkClusters
      .flatMap(_.nodeNames)
      // DON'T add duplicate nodes!!!
      .toSet[String]
      .map(
        name =>
          FakeNode(name = name,
                   port = -1,
                   user = "fake user",
                   password = "fake password",
                   services = Seq.empty,
                   lastModified = CommonUtil.current()))
      .foreach(store.add)
    clusterCollie(collie)
  }

  @Optional("default is implemented by ssh")
  def clusterCollie(clusterCollie: ClusterCollie): ConfiguratorBuilder = {
    if (this.clusterCollie.isDefined) throw new IllegalArgumentException(s"cluster collie is defined!!!")
    this.clusterCollie = Some(clusterCollie)
    this
  }

  private[this] def nodeCollie(): NodeCollie = new NodeCollie {
    override def node(name: String): Future[Node] = store.value[Node](name)
    override def nodes(): Future[Seq[Node]] = store.values[Node]
  }

  def build(): Configurator = {
    new Configurator(advertisedHostname, advertisedPort, initializationTimeout.get, terminationTimeout.get, extraRoute)(
      store = store,
      nodeCollie = nodeCollie(),
      clusterCollie = clusterCollie.getOrElse(ClusterCollie.ssh(nodeCollie()))
    )
  }
}
