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

import com.island.ohara.client.configurator.v0.InfoApi.ConnectorVersion
import com.island.ohara.client.configurator.v0.WorkerApi.WorkerClusterInfo

private[configurator] case class FakeWorkerClusterInfo(name: String,
                                                       imageName: String,
                                                       brokerClusterName: String,
                                                       clientPort: Int,
                                                       groupId: String,
                                                       statusTopicName: String,
                                                       statusTopicPartitions: Int,
                                                       statusTopicReplications: Short,
                                                       configTopicName: String,
                                                       configTopicPartitions: Int,
                                                       configTopicReplications: Short,
                                                       offsetTopicName: String,
                                                       offsetTopicPartitions: Int,
                                                       offsetTopicReplications: Short,
                                                       jarNames: Seq[String],
                                                       sources: Seq[ConnectorVersion],
                                                       sinks: Seq[ConnectorVersion],
                                                       nodeNames: Seq[String])
    extends WorkerClusterInfo
