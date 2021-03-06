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

package com.island.ohara.agent
import com.island.ohara.client.configurator.v0.BrokerApi.BrokerClusterInfo
import com.island.ohara.client.kafka.TopicAdmin
import com.island.ohara.common.annotations.Optional
import com.island.ohara.common.util.CommonUtil

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

trait BrokerCollie extends Collie[BrokerClusterInfo, BrokerCollie.ClusterCreator] {

  /**
    * Create a topic admin according to passed cluster name.
    * Noted: if target cluster doesn't exist, an future with exception will return
    * @param clusterName target cluster
    * @return cluster info and topic admin
    */
  def topicAdmin(clusterName: String): Future[(BrokerClusterInfo, TopicAdmin)] = cluster(clusterName).map {
    case (c, _) => (c, topicAdmin(c))
  }

  /**
    * Create a topic admin according to passed cluster.
    * @param cluster target cluster
    * @return topic admin
    */
  def topicAdmin(cluster: BrokerClusterInfo): TopicAdmin = TopicAdmin(cluster.connectionProps)
}

object BrokerCollie {
  trait ClusterCreator extends Collie.ClusterCreator[BrokerClusterInfo] {
    private[this] var clientPort: Int = CommonUtil.availablePort()
    private[this] var zookeeperClusterName: String = _
    private[this] var exporterPort: Int = CommonUtil.availablePort()

    def zookeeperClusterName(zookeeperClusterName: String): ClusterCreator = {
      this.zookeeperClusterName = CommonUtil.requireNonEmpty(zookeeperClusterName)
      this
    }

    @Optional("default is random port")
    def clientPort(clientPort: Int): ClusterCreator = {
      this.clientPort = CommonUtil.requirePositiveInt(clientPort)
      this
    }

    @Optional("default is random port")
    def exporterPort(exporterPort: Int): ClusterCreator = {
      this.exporterPort = CommonUtil.requirePositiveInt(exporterPort)
      this
    }

    override def create(): Future[BrokerClusterInfo] = doCreate(
      clusterName = CommonUtil.requireNonEmpty(clusterName),
      imageName = CommonUtil.requireNonEmpty(imageName),
      zookeeperClusterName = CommonUtil.requireNonEmpty(zookeeperClusterName),
      clientPort = CommonUtil.requirePositiveInt(clientPort),
      exporterPort = CommonUtil.requirePositiveInt(exporterPort),
      nodeNames = requireNonEmpty(nodeNames)
    )

    protected def doCreate(clusterName: String,
                           imageName: String,
                           zookeeperClusterName: String,
                           clientPort: Int,
                           exporterPort: Int,
                           nodeNames: Seq[String]): Future[BrokerClusterInfo]
  }

  private[agent] val ID_KEY: String = "BROKER_ID"
  private[agent] val DATA_DIRECTORY_KEY: String = "BROKER_DATA_DIR"
  private[agent] val ZOOKEEPERS_KEY: String = "BROKER_ZOOKEEPERS"
  private[agent] val CLIENT_PORT_KEY: String = "BROKER_CLIENT_PORT"
  private[agent] val ADVERTISED_HOSTNAME_KEY: String = "BROKER_ADVERTISED_HOSTNAME"
  private[agent] val ADVERTISED_CLIENT_PORT_KEY: String = "BROKER_ADVERTISED_CLIENT_PORT"
  private[agent] val EXPORTER_PORT_KEY: String = "PROMETHEUS_EXPORTER_PORT"
}
