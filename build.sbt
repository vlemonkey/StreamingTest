name := "StreamingTest"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.1",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.1",
  "org.apache.spark" % "spark-sql_2.10" % "1.3.1"
)
    