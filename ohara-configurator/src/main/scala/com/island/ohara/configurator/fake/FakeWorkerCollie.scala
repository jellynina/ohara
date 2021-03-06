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

package com.island.ohara.configurator.fake

import java.util.concurrent.ConcurrentHashMap

import com.island.ohara.agent.WorkerCollie
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo
import com.island.ohara.client.kafka.WorkerClient

import scala.concurrent.Future

private[configurator] class FakeWorkerCollie(wkConnectionProps: String)
    extends FakeCollie[WorkerClusterInfo, WorkerCollie.ClusterCreator]
    with WorkerCollie {

  /**
    * cache all connectors info in-memory so we should keep instance for each fake cluster.
    */
  private[this] val fakeClientCache = new ConcurrentHashMap[WorkerClusterInfo, FakeWorkerClient]
  override def creator(): WorkerCollie.ClusterCreator =
    (clusterName,
     imageName,
     brokerClusterName,
     clientPort,
     groupId,
     offsetTopicName,
     offsetTopicReplications,
     offsetTopicPartitions,
     statusTopicName,
     statusTopicReplications,
     statusTopicPartitions,
     configTopicName,
     configTopicReplications,
     _,
     nodeNames) =>
      Future.successful(
        addCluster(
          FakeWorkerClusterInfo(
            name = clusterName,
            imageName = imageName,
            brokerClusterName = brokerClusterName,
            clientPort = clientPort,
            groupId = groupId,
            offsetTopicName = offsetTopicName,
            offsetTopicPartitions = offsetTopicPartitions,
            offsetTopicReplications = offsetTopicReplications,
            configTopicName = configTopicName,
            configTopicPartitions = 1,
            configTopicReplications = configTopicReplications,
            statusTopicName = statusTopicName,
            statusTopicPartitions = statusTopicPartitions,
            statusTopicReplications = statusTopicReplications,
            jarNames = Seq.empty,
            sources = Seq.empty,
            sinks = Seq.empty,
            nodeNames = nodeNames
          )))

  override def removeNode(clusterName: String, nodeName: String): Future[WorkerClusterInfo] = {
    val previous = clusterCache.find(_._1.name == clusterName).get._1
    if (!previous.nodeNames.contains(nodeName))
      Future.failed(new IllegalArgumentException(s"$nodeName doesn't run on $clusterName!!!"))
    else
      Future.successful(
        addCluster(
          FakeWorkerClusterInfo(
            name = previous.name,
            imageName = previous.imageName,
            brokerClusterName = previous.brokerClusterName,
            clientPort = previous.clientPort,
            groupId = previous.groupId,
            statusTopicName = previous.statusTopicName,
            statusTopicPartitions = previous.statusTopicPartitions,
            statusTopicReplications = previous.statusTopicReplications,
            configTopicName = previous.configTopicName,
            configTopicPartitions = previous.configTopicPartitions,
            configTopicReplications = previous.configTopicReplications,
            offsetTopicName = previous.offsetTopicName,
            offsetTopicPartitions = previous.offsetTopicPartitions,
            offsetTopicReplications = previous.offsetTopicReplications,
            jarNames = previous.jarNames,
            sources = Seq.empty,
            sinks = Seq.empty,
            nodeNames = previous.nodeNames.filterNot(_ == nodeName)
          )))
  }

  override def addNode(clusterName: String, nodeName: String): Future[WorkerClusterInfo] = {
    val previous = clusterCache.find(_._1.name == clusterName).get._1
    if (previous.nodeNames.contains(nodeName))
      Future.failed(new IllegalArgumentException(s"$nodeName already run on $clusterName!!!"))
    else
      Future.successful(
        addCluster(
          FakeWorkerClusterInfo(
            name = previous.name,
            imageName = previous.imageName,
            brokerClusterName = previous.brokerClusterName,
            clientPort = previous.clientPort,
            groupId = previous.groupId,
            statusTopicName = previous.statusTopicName,
            statusTopicPartitions = previous.statusTopicPartitions,
            statusTopicReplications = previous.statusTopicReplications,
            configTopicName = previous.configTopicName,
            configTopicPartitions = previous.configTopicPartitions,
            configTopicReplications = previous.configTopicReplications,
            offsetTopicName = previous.offsetTopicName,
            offsetTopicPartitions = previous.offsetTopicPartitions,
            offsetTopicReplications = previous.offsetTopicReplications,
            jarNames = previous.jarNames,
            sources = Seq.empty,
            sinks = Seq.empty,
            nodeNames = previous.nodeNames :+ nodeName
          )))
  }
  override def workerClient(cluster: WorkerClusterInfo): WorkerClient =
    // < 0 means it is a fake cluster
    if (cluster.isInstanceOf[FakeWorkerClusterInfo]) {
      val fake = new FakeWorkerClient
      val r = fakeClientCache.putIfAbsent(cluster, fake)
      if (r == null) fake else r
    } else WorkerClient(wkConnectionProps)
}
