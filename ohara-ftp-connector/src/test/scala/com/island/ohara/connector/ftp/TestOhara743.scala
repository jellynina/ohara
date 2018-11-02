package com.island.ohara.connector.ftp

import com.island.ohara.client.FtpClient
import com.island.ohara.integration.FtpServer
import com.island.ohara.io.CloseOnce
import com.island.ohara.kafka.connector.TaskConfig
import com.island.ohara.rule.SmallTest
import org.junit.{After, Test}
import org.scalatest.Matchers

class TestOhara743 extends SmallTest with Matchers {

  private[this] val ftpServer = FtpServer.local(0, 0)

  @Test
  def testAutoCreateOutput(): Unit = {
    val props = FtpSourceProps(
      input = "/input",
      output = "/output",
      error = "/error",
      user = ftpServer.user,
      password = ftpServer.password,
      host = ftpServer.host,
      port = ftpServer.port,
      encode = Some("UTF-8")
    )

    val taskConfig = TaskConfig(
      name = "aa",
      topics = Seq.empty,
      schema = Seq.empty,
      options = props.toMap
    )

    val ftpClient = FtpClient
      .builder()
      .host(ftpServer.host)
      .port(ftpServer.port)
      .user(ftpServer.user)
      .password(ftpServer.password)
      .build()

    try {
      ftpClient.mkdir(props.input)
      val source = new FtpSource
      source._start(taskConfig)
      ftpClient.exist(props.error) shouldBe true
      ftpClient.exist(props.output) shouldBe true
    } finally ftpClient.close()
  }

  @After
  def tearDown(): Unit = {
    CloseOnce.close(ftpServer)
  }
}