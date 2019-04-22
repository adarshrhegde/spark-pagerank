name := "cs441_hw5"

version := "0.1"

//scalaVersion := "2.12.6"

scalaVersion := "2.11.0"

javacOptions in (Compile, compile) ++= Seq("-source", "1.7", "-target", "1.7")
scalacOptions := Seq("-target:jvm-1.7")

javaHome := Some(file("C:\\Program Files\\Java\\jdk1.7.0_80"))


lazy val slf4jVersion = "1.7.5"
lazy val logbackVersion = "1.2.3"
lazy val typeSafeConfigVersion = "1.2.1"
lazy val pureConfigVersion = "0.10.0"

libraryDependencies ++= Seq(
  "org.slf4j" % "slf4j-api" % slf4jVersion,
  "ch.qos.logback" % "logback-core" % logbackVersion,
  "ch.qos.logback" % "logback-classic" % logbackVersion,
  "org.apache.hadoop" % "hadoop-client" % "2.2.0",
  //"org.scala-lang.modules" %% "scala-xml" % "1.1.1",
  "com.typesafe" % "config" % "1.2.1",
  "org.apache.spark" %% "spark-core" % "2.3.2",
  "org.apache.spark" %% "spark-sql" % "2.3.2",
  "com.databricks" %% "spark-xml" % "0.5.0",
  "org.scalactic" %% "scalactic" % "3.0.5",
  "org.scalatest" %% "scalatest" % "3.0.5" % "test"

)

mainClass in (Compile, packageBin) := Some("com.uic.spark.PageRankMain")
mainClass in (Compile, run) := Some("com.uic.spark.PageRankMain")
mainClass in assembly := Some("com.uic.spark.PageRankMain")

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
