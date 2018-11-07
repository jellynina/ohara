package com.island.ohara.connector.ftp
import java.util.concurrent.TimeUnit

import com.island.ohara.client.ConfiguratorJson.Column
import com.island.ohara.client.FtpClient
import com.island.ohara.data.{Cell, Row}
import com.island.ohara.integration.{OharaTestUtil, With3Brokers3Workers}
import com.island.ohara.io.{ByteUtil, CloseOnce, IoUtil}
import com.island.ohara.kafka.{Consumer, KafkaClient, Producer}
import com.island.ohara.serialization.DataType
import org.junit.{Before, BeforeClass, Test}
import org.scalatest.Matchers

import scala.concurrent.duration._

object TestFtpSink extends With3Brokers3Workers with Matchers {

  private val TOPIC = "TestFtpSink"

  private val data = Row(Cell("a", "abc"), Cell("b", 123), Cell("c", true))

  @BeforeClass
  def init(): Unit = {
    this.setupData(TOPIC)
  }

  def setupData(topicName: String): Unit = {

    CloseOnce.doClose(KafkaClient(testUtil.brokersConnProps)) { client =>
      if (client.exist(topicName)) client.deleteTopic(topicName)
      client.topicCreator().numberOfPartitions(1).numberOfReplications(1).compacted().create(topicName)
    }

    CloseOnce.doClose(Producer.builder().brokers(testUtil.brokersConnProps).build[Array[Byte], Row])(
      _.sender().key(ByteUtil.toBytes("key")).value(data).send(topicName))

    CloseOnce.doClose(
      Consumer
        .builder()
        .topicName(topicName)
        .offsetFromBegin()
        .brokers(testUtil.brokersConnProps)
        .build[Array[Byte], Row]) { consumer =>
      val records = consumer.poll(60 seconds, 1)
      val row = records.head.value.get
      row.size shouldBe data.size
      row.cell("a").value shouldBe "abc"
      row.cell("b").value shouldBe 123
      row.cell("c").value shouldBe true
    }
  }

}

class TestFtpSink extends With3Brokers3Workers with Matchers {

  private[this] val TOPIC = TestFtpSink.TOPIC

  private[this] val schema: Seq[Column] = Seq(
    Column("a", DataType.STRING, 1),
    Column("b", DataType.INT, 2),
    Column("c", DataType.BOOLEAN, 3)
  )

  private[this] val data = TestFtpSink.data

  private[this] val props = FtpSinkProps(
    output = "/output",
    needHeader = false,
    user = testUtil.ftpServer.user,
    password = testUtil.ftpServer.password,
    host = testUtil.ftpServer.host,
    port = testUtil.ftpServer.port,
    encode = Some("UTF-8")
  )

  private[this] val ftpClient = FtpClient
    .builder()
    .hostname(testUtil.ftpServer.host)
    .port(testUtil.ftpServer.port)
    .user(testUtil.ftpServer.user)
    .password(testUtil.ftpServer.password)
    .build()
  @Before
  def setup(): Unit = {
    if (ftpClient.exist(props.output)) {
      ftpClient.listFileNames(props.output).map(IoUtil.path(props.output, _)).foreach(ftpClient.delete)
      ftpClient.listFileNames(props.output).size shouldBe 0
      ftpClient.delete(props.output)
    }
    ftpClient.mkdir(props.output)

    ftpClient.listFileNames(props.output).size shouldBe 0
  }

  @Test
  def testReorder(): Unit = {
    val topicName = TOPIC
    val connectorName = methodName
    val newSchema: Seq[Column] = Seq(
      Column("a", DataType.STRING, 3),
      Column("b", DataType.INT, 2),
      Column("c", DataType.BOOLEAN, 1)
    )
    testUtil.connectorClient
      .connectorCreator()
      .topic(topicName)
      .connectorClass(classOf[FtpSink])
      .numberOfTasks(1)
      .disableConverter()
      .name(connectorName)
      .schema(newSchema)
      .configs(props.toMap)
      .create()

    try {
      FtpUtil.checkConnector(testUtil, connectorName)
      OharaTestUtil.await(() => ftpClient.listFileNames(props.output).size == 1, 10 seconds)
      val lines = ftpClient.readLines(IoUtil.path(props.output, ftpClient.listFileNames(props.output).head))
      lines.length shouldBe 1
      val items = lines.head.split(",")
      items.length shouldBe data.size
      items(0) shouldBe data.cell(2).value.toString
      items(1) shouldBe data.cell(1).value.toString
      items(2) shouldBe data.cell(0).value.toString
    } finally testUtil.connectorClient.delete(connectorName)
  }

  @Test
  def testHeader(): Unit = {
    val topicName = TOPIC
    val connectorName = methodName
    testUtil.connectorClient
      .connectorCreator()
      .topic(topicName)
      .connectorClass(classOf[FtpSink])
      .numberOfTasks(1)
      .disableConverter()
      .name(connectorName)
      .schema(schema)
      .configs(props.copy(needHeader = true).toMap)
      .create()

    try {
      FtpUtil.checkConnector(testUtil, connectorName)
      OharaTestUtil.await(() => ftpClient.listFileNames(props.output).size == 1, 10 seconds)
      val lines = ftpClient.readLines(IoUtil.path(props.output, ftpClient.listFileNames(props.output).head))
      lines.length shouldBe 2
      lines.head shouldBe schema.sortBy(_.order).map(_.name).mkString(",")
      val items = lines(1).split(",")
      items.length shouldBe data.size
      items(0) shouldBe data.cell(0).value.toString
      items(1) shouldBe data.cell(1).value.toString
      items(2) shouldBe data.cell(2).value.toString
    } finally testUtil.connectorClient.delete(connectorName)
  }

  @Test
  def testHeaderWithoutSchema(): Unit = {
    val topicName = TOPIC
    val connectorName = methodName
    testUtil.connectorClient
      .connectorCreator()
      .topic(topicName)
      .connectorClass(classOf[FtpSink])
      .numberOfTasks(1)
      .disableConverter()
      .name(connectorName)
      .configs(props.copy(needHeader = true).toMap)
      .create()

    try {
      FtpUtil.checkConnector(testUtil, connectorName)
      OharaTestUtil.await(() => ftpClient.listFileNames(props.output).size == 1, 10 seconds)
      val lines = ftpClient.readLines(IoUtil.path(props.output, ftpClient.listFileNames(props.output).head))
      lines.length shouldBe 2
      lines.head shouldBe data.map(_.name).mkString(",")
      val items = lines(1).split(",")
      items.length shouldBe data.size
      items(0) shouldBe data.cell(0).value.toString
      items(1) shouldBe data.cell(1).value.toString
      items(2) shouldBe data.cell(2).value.toString
    } finally testUtil.connectorClient.delete(connectorName)
  }

  @Test
  def testColumnRename(): Unit = {
    val topicName = TOPIC
    val connectorName = methodName
    val schema = Seq(
      Column("a", "aa", DataType.STRING, 1),
      Column("b", "bb", DataType.INT, 2),
      Column("c", "cc", DataType.BOOLEAN, 3)
    )
    testUtil.connectorClient
      .connectorCreator()
      .topic(topicName)
      .connectorClass(classOf[FtpSink])
      .numberOfTasks(1)
      .disableConverter()
      .name(connectorName)
      .schema(schema)
      .configs(props.copy(needHeader = true).toMap)
      .create()

    try {
      FtpUtil.checkConnector(testUtil, connectorName)
      OharaTestUtil.await(() => ftpClient.listFileNames(props.output).size == 1, 10 seconds)
      val lines = ftpClient.readLines(IoUtil.path(props.output, ftpClient.listFileNames(props.output).head))
      lines.length shouldBe 2
      lines.head shouldBe schema.sortBy(_.order).map(_.newName).mkString(",")
      val items = lines(1).split(",")
      items.length shouldBe data.size
      items(0) shouldBe data.cell(0).value.toString
      items(1) shouldBe data.cell(1).value.toString
      items(2) shouldBe data.cell(2).value.toString
    } finally testUtil.connectorClient.delete(connectorName)
  }

  @Test
  def testNormalCase(): Unit = {
    val topicName = TOPIC
    val connectorName = methodName
    testUtil.connectorClient
      .connectorCreator()
      .topic(topicName)
      .connectorClass(classOf[FtpSink])
      .numberOfTasks(1)
      .disableConverter()
      .name(connectorName)
      .schema(schema)
      .configs(props.toMap)
      .create()

    try {
      FtpUtil.checkConnector(testUtil, connectorName)
      OharaTestUtil.await(() => ftpClient.listFileNames(props.output).size == 1, 10 seconds)
      val lines = ftpClient.readLines(IoUtil.path(props.output, ftpClient.listFileNames(props.output).head))
      lines.length shouldBe 1
      val items = lines.head.split(",")
      items.length shouldBe data.size
      items(0) shouldBe data.cell(0).value.toString
      items(1) shouldBe data.cell(1).value.toString
      items(2) shouldBe data.cell(2).value.toString
    } finally testUtil.connectorClient.delete(connectorName)
  }

  @Test
  def testNormalCaseWithoutSchema(): Unit = {
    val topicName = TOPIC
    val connectorName = methodName
    testUtil.connectorClient
      .connectorCreator()
      .topic(topicName)
      .connectorClass(classOf[FtpSink])
      .numberOfTasks(1)
      .disableConverter()
      .name(connectorName)
      .configs(props.toMap)
      .create()

    try {
      FtpUtil.checkConnector(testUtil, connectorName)
      OharaTestUtil.await(() => ftpClient.listFileNames(props.output).size == 1, 10 seconds)
      val lines = ftpClient.readLines(IoUtil.path(props.output, ftpClient.listFileNames(props.output).head))
      lines.length shouldBe 1
      val items = lines.head.split(",")
      items.length shouldBe data.size
      items(0) shouldBe data.cell(0).value.toString
      items(1) shouldBe data.cell(1).value.toString
      items(2) shouldBe data.cell(2).value.toString
    } finally testUtil.connectorClient.delete(connectorName)
  }

  @Test
  def testPartialColumns(): Unit = {
    val topicName = TOPIC
    val connectorName = methodName
    testUtil.connectorClient
      .connectorCreator()
      .topic(topicName)
      .connectorClass(classOf[FtpSink])
      .numberOfTasks(1)
      .disableConverter()
      .name(connectorName)
      // skip last column
      .schema(schema.slice(0, schema.length - 1))
      .configs(props.toMap)
      .create()

    try {
      FtpUtil.checkConnector(testUtil, connectorName)
      OharaTestUtil.await(() => ftpClient.listFileNames(props.output).size == 1, 10 seconds)
      val lines = ftpClient.readLines(IoUtil.path(props.output, ftpClient.listFileNames(props.output).head))
      lines.length shouldBe 1
      val items = lines.head.split(",")
      items.length shouldBe data.size - 1
      items(0) shouldBe data.cell(0).value.toString
      items(1) shouldBe data.cell(1).value.toString
    } finally testUtil.connectorClient.delete(connectorName)
  }

  @Test
  def testUnmatchedSchema(): Unit = {
    val topicName = TOPIC
    val connectorName = methodName
    testUtil.connectorClient
      .connectorCreator()
      .topic(topicName)
      .connectorClass(classOf[FtpSink])
      .numberOfTasks(1)
      .disableConverter()
      .name(connectorName)
      // the name can't be casted to int
      .schema(Seq(Column("name", DataType.INT, 1)))
      .configs(props.toMap)
      .create()

    try {
      FtpUtil.checkConnector(testUtil, connectorName)
      TimeUnit.SECONDS.sleep(2)
      ftpClient.listFileNames(props.output).size shouldBe 0
    } finally testUtil.connectorClient.delete(connectorName)
  }
}
