package com.test

import java.util.Properties

import kafka.producer._

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._

/**
 * Created by s on 15-6-4.
 */
/**
 * Consumes messages from one or more topics in Kafka and does wordcount.
 * Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>
 *   <zkQuorum> is a list of one or more zookeeper servers that make quorum
 *   <group> is the name of kafka consumer group
 *   <topics> is a list of one or more kafka topics to consume from
 *   <numThreads> is the number of threads the kafka consumer should use
 *
 * Example:
 *    `$ bin/run-example \
 *      org.apache.spark.examples.streaming.KafkaWordCount zoo01,zoo02,zoo03 \
 *      my-consumer-group topic1,topic2 1`
 *    `spark-submit --master spark://master2:7077 --jars spark-streaming-kafka_2.10-1.2.0-cdh5.3.0.jar
 *      --class com.test.StreamingTest streamingtest_2.10-1.0.jar slave1:2181 my-consumer-group test2 1`
 */

object StreamingTest {
  def main (args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val conf = new SparkConf().setAppName("Streaming Test")
    val ssc = new StreamingContext(conf, Seconds(10))
    ssc.checkpoint("checkpoint")

    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    val words = lines.flatMap(_.split(","))
    val wordCounts = words.map((_, 1L)).reduceByKeyAndWindow(_+_, _-_, Minutes(1), Seconds(10), 2)
    wordCounts.print()

    ssc.start()
    ssc.awaitTermination()

  }
}


// Produces some random words between 1 and 100
/**
 * `spark-submit --master spark://master2:7077 --class com.test.KafkaWordCountProducer
      streamingtest_2.10-1.0.jar  slave2:9092,master2:9092 test2 100 100`
 */
object KafkaWordCountProducer {
  def main (args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCountProducer <metadataBrokerList> <topic> " +
        "<messagesPerSec> <wordsPerMessage>")
      System.exit(1)
    }

    val Array(brokers, topic, messagesPerSec, wordsPerMessage) = args

    // Zookeeper connection properties
    val props = new Properties()
    props.put("metadata.broker.list", brokers)
    props.put("serializer.class", "kafka.serializer.StringEncoder")

    val config = new ProducerConfig(props)
    val producer = new Producer[String, String](config)

    // Send some messages
    while (true) {
      val messages = (1 to messagesPerSec.toInt).map { messageNum =>
        val str = (1 to wordsPerMessage.toInt).map(x => scala.util.Random.nextInt(10).toString)
          .mkString(",")
        new KeyedMessage[String, String](topic, str)
      }.toArray

      producer.send(messages: _*)
      Thread.sleep(100)
    }

  }
}