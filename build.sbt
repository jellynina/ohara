import java.lang.Runtime

name := "ohara"

version := "0.0.1"

organizationName := "is-land Systems Inc."

organizationHomepage := Some(url("https://github.com/is-land"))

organization := "com.island"

val cpus = Runtime.getRuntime().availableProcessors().toString

scalacOptions ++= Seq(
  // Scala Compiler Options
  // https://github.com/scala/scala/blob/2.12.x/src/compiler/scala/tools/nsc/settings/StandardScalaSettings.scala
  // https://github.com/scala/scala/blob/2.12.x/src/compiler/scala/tools/nsc/settings/ScalaSettings.scala
  "-deprecation",
  "-unchecked",
  "-encoding", "utf8",
  "-Xlog-reflective-calls",
  "-feature",
  "-language:postfixOps",
  "-language:implicitConversions",
  "-language:existentials",
  "-Xlint:by-name-right-associative",
  "-Xlint:delayedinit-select",
  "-Xlint:doc-detached",
  "-Xlint:missing-interpolator",
  "-Xlint:nullary-override",
  "-Xlint:nullary-unit",
  "-Xlint:option-implicit",
  "-Xlint:package-object-classes",
  "-Xlint:poly-implicit-overload",
  "-Xlint:private-shadow",
  "-Xlint:stars-align",
  "-Xlint:type-parameter-shadow",
  "-Xlint:unsound-match",
  "-target:jvm-1.8",
  "-explaintypes",
  "-feature",
  "-unchecked",
  "-Xlint",
  "-Ywarn-dead-code",
  "-Ywarn-unused:implicits",
  "-Ywarn-unused:imports",
  "-Ywarn-unused:locals",
  "-Ywarn-unused:params",
  "-Ywarn-unused:patvars",
  "-Ywarn-unused:privates",
  "-Ywarn-value-discard",
  "-Ybackend-parallelism", cpus
)

lazy val akkaHttpV = "10.1.1"
lazy val akkaV = "2.5.12"
lazy val kafkaV = "1.0.1"
lazy val slf4jV = "1.7.25"

resolvers += Resolver.bintrayRepo("cakesolutions", "maven")

val formatAll   = taskKey[Unit]("Format all the source code which includes src, test, and build files")
val checkFormat = taskKey[Unit]("Check all the source code which includes src, test, and build files")

lazy val commonSettings = Seq(
  scalaVersion := "2.12.6",
  libraryDependencies ++= Seq(
    // kafka
    "org.apache.kafka" %% "kafka" % kafkaV,
    "org.apache.kafka" % "kafka-clients" % kafkaV,
    "org.apache.kafka" % "connect-api" % kafkaV,
    "org.apache.kafka" % "connect-file" % kafkaV,
    "org.apache.kafka" % "connect-runtime" % kafkaV,
    "org.apache.kafka" % "connect-json" % kafkaV,

    // 3rd for kafka
    "net.cakesolutions" %% "scala-kafka-client" % "1.1.0",

    // akka
    "com.typesafe.akka" %% "akka-actor" % akkaV,
    "com.typesafe.akka" %% "akka-stream" % akkaV,
    "com.typesafe.akka" %% "akka-slf4j" % akkaV,

    // akka-http
    "com.typesafe.akka" %% "akka-http"            % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-spray-json" % akkaHttpV,
    "com.typesafe.akka" %% "akka-http-xml"        % akkaHttpV,

    // log
    "org.slf4j" % "slf4j-api" % slf4jV,
    "org.slf4j" % "slf4j-log4j12" % slf4jV,

    // http
    "org.apache.httpcomponents" % "httpclient" % "4.5.5",

    // misc
    "com.typesafe" % "config" % "1.3.3",
    "com.typesafe.scala-logging" %% "scala-logging" % "3.9.0",

    // test
    "org.scalatest" %% "scalatest" % "3.0.5" % Test,
    "com.typesafe.akka" %% "akka-http-testkit"    % akkaHttpV % Test,
    "com.typesafe.akka" %% "akka-testkit"         % akkaV     % Test,
    "com.typesafe.akka" %% "akka-stream-testkit" % akkaV % Test,
    "org.mockito" % "mockito-all" % "1.10.19" % Test,
    "junit" % "junit" % "4.12" % Test,

    // integration testing
    "org.apache.zookeeper" % "zookeeper" % "3.4.10"
  ),
  scalafmtOnCompile := true,
  formatAll := {
    (scalafmt in Compile).value
    (scalafmt in Test).value
    (scalafmtSbt in Compile).value
  },
  checkFormat := {
    (scalafmtCheck in Compile).value
    (scalafmtCheck in Test).value
    (scalafmtSbtCheck in Compile).value
  }
)

lazy val `ohara-common` = (project in file("ohara-common"))
  .settings(commonSettings)

lazy val `ohara-core` = (project in file("ohara-core"))
  .settings(commonSettings)

lazy val `ohara-data` = (project in file("ohara-data"))
  .dependsOn(`ohara-common` % "compile->compile; compile->test")
  .settings(commonSettings)

lazy val `ohara-kafka-data-wrapper` = (project in file("ohara-kafka-data"))
  .settings(commonSettings)
  .dependsOn(
    `ohara-common` % "compile->compile; compile->test",
    `ohara-core`,
    `ohara-data`,
    `testkit` % "compile->compile; compile->test")

lazy val `ohara-manager` = (project in file("ohara-manager"))
  .settings(commonSettings)

lazy val `ohara-hdfs-sink` = (project in file("ohara-hdfs-sink-connector"))
  .settings(commonSettings)
  .dependsOn(`ohara-data`, `ohara-kafka-data-wrapper`)

lazy val `ohara-http` = (project in file("ohara-http"))
  .settings(commonSettings)

lazy val `testkit` = (project in file("ohara-testing-util"))
  .settings(commonSettings)
  .dependsOn(`ohara-common` % "compile->compile; compile->test")