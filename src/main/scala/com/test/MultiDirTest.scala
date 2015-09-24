package com.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by s on 15-7-1.
 */
object MultiDirTest {
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: MultiDirTest  <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThread) = args
    val conf = new SparkConf().setAppName("MultiDir Output Test")
    val ssc = new StreamingContext(conf, Seconds(10))

    val topicMap = topics.split(",").map((_, numThread.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)


  }
}
