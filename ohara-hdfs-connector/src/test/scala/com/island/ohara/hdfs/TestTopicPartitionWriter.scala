package com.island.ohara.hdfs

import java.io.OutputStream
import java.util

import com.island.ohara.core.{Cell, Row}
import com.island.ohara.hdfs.storage.{HDFSStorage, Storage}
import com.island.ohara.hdfs.text.RecordWriterOutput
import com.island.ohara.integration.OharaTestUtil
import com.island.ohara.kafka.connector.RowSinkRecord
import com.island.ohara.rule.MediumTest
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.connect.sink.SinkTaskContext
import org.junit.Test
import org.mockito.Matchers._
import org.mockito.Mockito._
import org.scalatest.Matchers
import org.scalatest.mockito.MockitoSugar

class TestTopicPartitionWriter extends MediumTest with Matchers with MockitoSugar {

  @Test
  def testOpenTempFile(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")
    props.put(HDFSSinkConnectorConfig.TMP_DIR, "/tmp")

    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)
    val sinkTaskContext = mock[SinkTaskContext]
    val topicPartition = new TopicPartition("topic1", 0)
    val recordWriterProvider = mock[RecordWriterOutput]

    val testUtil = OharaTestUtil.localHDFS(1)
    val storage = new HDFSStorage(testUtil.fileSystem())
    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)
    new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    topicPartitionWriter.openTempFile(recordWriterProvider, 0)
    topicPartitionWriter.processLineCount shouldBe 0
    storage.exists(topicPartitionWriter.tmpFilePath) shouldBe true
  }

  @Test
  def testWriteData(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")
    props.put(HDFSSinkConnectorConfig.TMP_DIR, "/tmp")

    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)

    val sinkTaskContext = mock[SinkTaskContext]
    val topicPartition = new TopicPartition("topic1", 0)
    val storage = mock[Storage]
    when(storage.open(anyString(), anyBoolean())).thenReturn(mock[OutputStream])

    val recordWriterOutput = mock[RecordWriterOutput]
    val outputStream: OutputStream = mock[OutputStream]
    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    val rowSinkRecord = mock[RowSinkRecord]
    when(rowSinkRecord.value).thenReturn(Row(Cell.builder.name("column1").build("value")))

    topicPartitionWriter.recordWriterOutput = recordWriterOutput
    topicPartitionWriter.write(rowSinkRecord)
    topicPartitionWriter.processLineCount shouldBe 1
    topicPartitionWriter.write(rowSinkRecord)
    topicPartitionWriter.processLineCount shouldBe 2
  }

  @Test
  def testDefaultValueCommitFile(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")
    props.put(HDFSSinkConnectorConfig.TMP_DIR, "/tmp")

    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)
    val sinkTaskContext = mock[SinkTaskContext]

    val topicPartition = new TopicPartition("topic1", 1)
    val storage = mock[Storage]
    when(storage.open(anyString(), anyBoolean())).thenReturn(mock[OutputStream])
    when(storage.list("/data/partition1"))
      .thenReturn(List("prefix-000000001-000001000.csv", "prefix-000001001-000002000.csv").toIterator)

    val recordWriterOutput = mock[RecordWriterOutput]
    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    val rowSinkRecord = mock[RowSinkRecord]
    when(rowSinkRecord.value).thenReturn(Row(Cell.builder.name("column1").build("value")))

    topicPartitionWriter.recordWriterOutput = recordWriterOutput
    for (i <- 1 to 998) {
      topicPartitionWriter.write(rowSinkRecord)
      topicPartitionWriter.processLineCount shouldBe i
    }
    topicPartitionWriter.write(rowSinkRecord)
    topicPartitionWriter.processLineCount shouldBe 999
  }

  @Test
  def testIsDataCountCommit(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")
    props.put(HDFSSinkConnectorConfig.TMP_DIR, "/tmp")

    val topicPartition = new TopicPartition("topic1", 0)

    val sinkTaskContext = mock[SinkTaskContext]
    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)
    val storage = mock[Storage]
    when(storage.list("/data/partition0"))
      .thenReturn(List("prefix-000000001-000001000.csv", "prefix-000001001-000002000.csv").toIterator)

    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    topicPartitionWriter.isDataCountCommit(0, 100) shouldBe false
    topicPartitionWriter.isDataCountCommit(10, 100) shouldBe false
    topicPartitionWriter.isDataCountCommit(100, 100) shouldBe true
    topicPartitionWriter.isDataCountCommit(101, 100) shouldBe true
  }

  @Test
  def testIsTimeCommit(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")
    props.put(HDFSSinkConnectorConfig.TMP_DIR, "/tmp")

    val topicPartition = new TopicPartition("topic1", 0)

    val sinkTaskContext = mock[SinkTaskContext]
    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)
    val storage = mock[Storage]
    when(storage.list("/data/partition0"))
      .thenReturn(List("prefix-000000001-000001000.csv", "prefix-000001001-000002000.csv").toIterator)

    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    val startTime: Long = System.currentTimeMillis() - 1000
    topicPartitionWriter.isTimeCommit(startTime, 2000) shouldBe false //Not commit
    topicPartitionWriter.isTimeCommit(startTime, 1000) shouldBe true
  }

  @Test
  def testWriteByTime(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")
    props.put(HDFSSinkConnectorConfig.TMP_DIR, "/tmp")
    props.put(HDFSSinkConnectorConfig.ROTATE_INTERVAL_MS, "1500")

    val topicPartition = new TopicPartition("topic1", 0)

    val sinkTaskContext = mock[SinkTaskContext]
    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)
    val storage = mock[Storage]
    when(storage.open(anyString(), anyBoolean())).thenReturn(mock[OutputStream])
    when(storage.list("/data/partition0"))
      .thenReturn(List("prefix-000000001-000001000.csv", "prefix-000001001-000002000.csv").toIterator)

    val recordWriterOutput = mock[RecordWriterOutput]
    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    val rowSinkRecord = mock[RowSinkRecord]
    when(rowSinkRecord.value).thenReturn(Row(Cell.builder.name("column1").build("value")))

    topicPartitionWriter.recordWriterOutput = recordWriterOutput
    topicPartitionWriter.processLineCount = 0
    topicPartitionWriter.write(rowSinkRecord)
  }

  @Test
  def testFlushFilePath1(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")
    props.put(HDFSSinkConnectorConfig.TMP_DIR, "/tmp")
    props.put(HDFSSinkConnectorConfig.ROTATE_INTERVAL_MS, "1500")

    val topicPartition = new TopicPartition("topic1", 0)

    val sinkTaskContext = mock[SinkTaskContext]
    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)
    val storage = mock[Storage]

    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    val dataDir = "/data/partition1"
    val result = topicPartitionWriter.flushFilePath(Iterator(), dataDir)
    result shouldBe "/data/partition1/part-000000000-000000000.csv"
  }

  @Test
  def testFlushFilePath2(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")

    val topicPartition = new TopicPartition("topic1", 0)
    val sinkTaskContext = mock[SinkTaskContext]
    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)
    val storage = mock[Storage]

    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    topicPartitionWriter.processLineCount = 1000
    val dataDir = "/data/partition1"
    var flushFilePath = topicPartitionWriter.flushFilePath(Iterator(), dataDir)
    flushFilePath shouldBe "/data/partition1/part-000000000-000000999.csv"

    flushFilePath = topicPartitionWriter.flushFilePath(Iterator("part-000000000-000000999.csv"), dataDir)
    flushFilePath shouldBe "/data/partition1/part-000001000-000001999.csv"

    flushFilePath = topicPartitionWriter
      .flushFilePath(Iterator("part-000000000-000000999.csv", "part-000001000-000001999.csv"), dataDir)
    flushFilePath shouldBe "/data/partition1/part-000002000-000002999.csv"

    flushFilePath = topicPartitionWriter.flushFilePath(Iterator("part-000002000-000002999.csv"), dataDir)
    flushFilePath shouldBe "/data/partition1/part-000003000-000003999.csv"
  }

  @Test
  def testFlushFilePath3(): Unit = {
    val props = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, "")

    val topicPartition = new TopicPartition("topic1", 0)
    val sinkTaskContext = mock[SinkTaskContext]
    val hdfsSinkConnectorConfig = new HDFSSinkConnectorConfig(props)
    val storage = mock[Storage]

    val topicPartitionWriter =
      new TopicPartitionWriter(hdfsSinkConnectorConfig, sinkTaskContext, topicPartition, storage)

    val dataDir = "/data/partition1"

    topicPartitionWriter.processLineCount = 1000
    var flushFilePath = topicPartitionWriter.flushFilePath(Iterator(), dataDir)
    flushFilePath shouldBe "/data/partition1/part-000000000-000000999.csv"

    topicPartitionWriter.processLineCount = 500
    flushFilePath = topicPartitionWriter.flushFilePath(Iterator("part-000000000-000000999.csv"), dataDir)
    flushFilePath shouldBe "/data/partition1/part-000001000-000001499.csv"

    topicPartitionWriter.processLineCount = 222
    flushFilePath = topicPartitionWriter.flushFilePath(Iterator("part-000001000-000001499.csv"), dataDir)
    flushFilePath shouldBe "/data/partition1/part-000001500-000001721.csv"
  }
}