package com.island.ohara.connector.hdfs

import java.util
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.island.ohara.connector.hdfs.creator.LocalHDFSStorageCreator
import com.island.ohara.connector.hdfs.storage.HDFSStorage
import com.island.ohara.data.{Cell, Row}
import com.island.ohara.integration._
import com.island.ohara.io.ByteUtil
import com.island.ohara.io.CloseOnce._
import com.island.ohara.kafka.Producer
import com.island.ohara.kafka.connector.RowSinkTask
import com.island.ohara.serialization.Serializer
import org.apache.hadoop.fs.Path
import org.junit.Test
import org.scalatest.Matchers

import scala.concurrent.duration._

class TestHDFSSinkConnector extends With3Brokers3Workers3DataNodes with Matchers {
  private[this] val hdfsURL: String = "hdfs://host1:9000"
  private[this] val tmpDir: String = "/tmp"

  @Test
  def testTaskConfigs(): Unit = {
    val maxTasks = 5
    val hdfsSinkConnector = new HDFSSinkConnector()
    val props: util.Map[String, String] = new util.HashMap[String, String]()
    props.put(HDFSSinkConnectorConfig.HDFS_URL, hdfsURL)
    props.put(HDFSSinkConnectorConfig.TMP_DIR, tmpDir)

    hdfsSinkConnector.start(props)
    val result: util.List[util.Map[String, String]] = hdfsSinkConnector.taskConfigs(maxTasks)

    result.size() shouldBe maxTasks
    result.get(0).get(HDFSSinkConnectorConfig.HDFS_URL) shouldBe hdfsURL
    result.get(0).get(HDFSSinkConnectorConfig.TMP_DIR) shouldBe tmpDir
  }

  @Test
  def testRunMiniClusterAndAssignConfig(): Unit = {
    val connectorName = methodName
    val topicName = methodName
    val sinkTasks = 2
    val flushLineCountName = HDFSSinkConnectorConfig.FLUSH_LINE_COUNT
    val flushLineCount = "2000"
    val rotateIntervalMSName = HDFSSinkConnectorConfig.ROTATE_INTERVAL_MS
    val tmpDirName = HDFSSinkConnectorConfig.TMP_DIR
    val tmpDirPath = "/home/tmp"
    val hdfsURLName = HDFSSinkConnectorConfig.HDFS_URL

    val localURL = s"file://${testUtil.tmpDirectory}"
    testUtil.connectorClient
      .sinkConnectorCreator()
      .name(connectorName)
      .connectorClass(classOf[SimpleHDFSSinkConnector])
      .topic(topicName)
      .taskNumber(sinkTasks)
      .disableConverter()
      .config(Map(flushLineCountName -> flushLineCount, tmpDirName -> tmpDirPath, hdfsURLName -> localURL))
      .build()

    OharaTestUtil.await(() => SimpleHDFSSinkTask.taskProps.get("topics") == connectorName, 20 second)
    OharaTestUtil.await(() => SimpleHDFSSinkTask.taskProps.get(flushLineCountName) == flushLineCount, 20 second)
    OharaTestUtil.await(() => SimpleHDFSSinkTask.taskProps.get(rotateIntervalMSName) == null, 20 second)
    OharaTestUtil.await(() => SimpleHDFSSinkTask.taskProps.get(tmpDirName) == tmpDirPath, 10 second)
    OharaTestUtil.await(() => SimpleHDFSSinkTask.sinkConnectorConfig.dataDir() == "/data", 20 second)
    OharaTestUtil.await(() => SimpleHDFSSinkTask.sinkConnectorConfig.flushLineCount() == 2000, 20 second)
    OharaTestUtil.await(() => !SimpleHDFSSinkTask.sinkConnectorConfig.offsetInconsistentSkip(), 20 second)
  }

  @Test
  def testHDFSSinkConnector(): Unit = {
    val sinkTasks = 1
    val flushLineCountName = HDFSSinkConnectorConfig.FLUSH_LINE_COUNT
    val flushLineCount = "10"
    val tmpDirName = HDFSSinkConnectorConfig.TMP_DIR
    val dataDirName = HDFSSinkConnectorConfig.DATA_DIR
    val hdfsURLName = HDFSSinkConnectorConfig.HDFS_URL
    val connectorName = methodName
    val topicName = methodName
    val rowCount = 100
    val row = Row.builder.append(Cell.builder.name("cf0").build(10)).append(Cell.builder.name("cf1").build(11)).build()
    val hdfsCreatorClassName = HDFSSinkConnectorConfig.HDFS_STORAGE_CREATOR_CLASS
    val hdfsCreatorClassNameValue = classOf[LocalHDFSStorageCreator].getName

    val fileSystem = testUtil.fileSystem
    val storage = new HDFSStorage(fileSystem)
    val tmpDirPath = s"${testUtil.tmpDirectory}/tmp"
    val dataDirPath = s"${testUtil.tmpDirectory}/data"
    doClose(Producer.builder(Serializer.BYTES, Serializer.ROW).brokers(testUtil.brokers).build()) { producer =>
      {
        0 until rowCount foreach (_ =>
          producer.sender().topic(topicName).key(ByteUtil.toBytes("key")).value(row).send())
        producer.flush()
      }
    }

    val localURL = s"file://${testUtil.tmpDirectory}"
    testUtil.connectorClient
      .sinkConnectorCreator()
      .name(connectorName)
      .connectorClass(classOf[HDFSSinkConnector])
      .topic(topicName)
      .taskNumber(sinkTasks)
      .disableConverter()
      .config(Map(
        flushLineCountName -> flushLineCount,
        tmpDirName -> tmpDirPath,
        hdfsURLName -> localURL,
        hdfsCreatorClassName -> hdfsCreatorClassNameValue,
        dataDirName -> dataDirPath
      ))
      .build()

    TimeUnit.SECONDS.sleep(5)
    val partitionID: String = "partition0"
    OharaTestUtil.await(() => storage.list(s"$dataDirPath/$topicName/$partitionID").size == 10, 10 seconds)

    OharaTestUtil.await(
      () =>
        FileUtils.getStopOffset(storage.list(s"$dataDirPath/$topicName/$partitionID").map(FileUtils.fileName(_))) == 99,
      10 seconds)

    OharaTestUtil.await(() =>
                          storage
                            .list(s"$dataDirPath/$topicName/$partitionID")
                            .map(FileUtils.fileName)
                            .contains("part-000000090-000000099.csv"),
                        10 seconds)
  }

  @Test
  def testRecoverOffset(): Unit = {
    val sinkTasks = 1
    val flushLineCountName = HDFSSinkConnectorConfig.FLUSH_LINE_COUNT
    val flushLineCount = "100"
    val tmpDirName = HDFSSinkConnectorConfig.TMP_DIR
    val dataDirName = HDFSSinkConnectorConfig.DATA_DIR
    val hdfsURLName = HDFSSinkConnectorConfig.HDFS_URL
    val connectorName = methodName
    val topicName = methodName
    val rowCount = 200
    val row = Row.builder.append(Cell.builder.name("cf0").build(10)).append(Cell.builder.name("cf1").build(11)).build()
    val hdfsCreatorClassName = HDFSSinkConnectorConfig.HDFS_STORAGE_CREATOR_CLASS
    val hdfsCreatorClassNameValue = classOf[LocalHDFSStorageCreator].getName

    val fileSystem = testUtil.fileSystem
    val storage = new HDFSStorage(fileSystem)
    val tmpDirPath = s"${testUtil.tmpDirectory}/tmp"
    val dataDirPath = s"${testUtil.tmpDirectory}/data"

    //Before running the Kafka Connector, create the file to local hdfs for test recover offset
    val partitionID: String = "partition0"
    fileSystem.createNewFile(new Path(s"$dataDirPath/$topicName/$partitionID/part-000000000-000000099.csv"))

    doClose(Producer.builder(Serializer.BYTES, Serializer.ROW).brokers(testUtil.brokers).build()) { producer =>
      {
        0 until rowCount foreach (_ =>
          producer.sender().topic(topicName).key(ByteUtil.toBytes("key")).value(row).send())
        producer.flush()
      }
    }

    val localURL = s"file://${testUtil.tmpDirectory}"
    testUtil.connectorClient
      .sinkConnectorCreator()
      .name(connectorName)
      .connectorClass(classOf[HDFSSinkConnector])
      .topic(topicName)
      .taskNumber(sinkTasks)
      .disableConverter
      .config(Map(
        flushLineCountName -> flushLineCount,
        tmpDirName -> tmpDirPath,
        hdfsURLName -> localURL,
        hdfsCreatorClassName -> hdfsCreatorClassNameValue,
        dataDirName -> dataDirPath
      ))
      .build()

    TimeUnit.SECONDS.sleep(5)
    storage
      .list(s"$dataDirPath/$topicName/$partitionID")
      .foreach(x => {
        println(s"file path: $x")
      })
    OharaTestUtil.await(() => storage.list(s"$dataDirPath/$topicName/$partitionID").size == 2, 10 seconds)

    OharaTestUtil.await(() =>
                          FileUtils.getStopOffset(
                            storage.list(s"$dataDirPath/$topicName/$partitionID").map(FileUtils.fileName(_))) == 199,
                        10 seconds)

    OharaTestUtil.await(() =>
                          storage
                            .list(s"$dataDirPath/$topicName/$partitionID")
                            .map(FileUtils.fileName)
                            .contains("part-000000100-000000199.csv"),
                        10 seconds)
  }
}

class SimpleHDFSSinkConnector extends HDFSSinkConnector {
  override protected def _taskClass(): Class[_ <: RowSinkTask] = {
    classOf[SimpleHDFSSinkTask]
  }
}

class SimpleHDFSSinkTask extends HDFSSinkTask {
  override def _start(props: Map[String, String]): Unit = {
    super._start(props)
    props.foreach {
      case (k, v) => SimpleHDFSSinkTask.taskProps.put(k, v)
    }
    SimpleHDFSSinkTask.sinkConnectorConfig = hdfsSinkConnectorConfig
  }
}

object SimpleHDFSSinkTask {
  val taskProps = new ConcurrentHashMap[String, String]
  var sinkConnectorConfig: HDFSSinkConnectorConfig = _
}