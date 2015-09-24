package com.test

import java.text.SimpleDateFormat
import java.util.{Properties, Calendar}

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import scala.collection.immutable.Map

import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.{KafkaUtils}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by wangxy on 15-6-30.
 */

object mConsumer {

  /**
   * spark-submit --master spark://master2:7077 --jars spark-streaming-kafka_2.10-1.3.1.jar \
   * --class com.test.mConsumer 5 master2:2181,slave1:2181,slave2:2181 mytest baotest 1 bao/test
   */
  def main (args: Array[String]) {
//    if (args.length < 6) {
//      System.err.println("Usage: KafkaWordCount <ssTime> <zkQuorum> <group> <topics> <numThreads> <output>")
//      System.exit(1)
//    }

//    val Array(ssTime, zkQuorum, group, topics, numThreads, strPath) = args
    val Array(ssTime, zkQuorum, group, topics, numThreads, strPath) =
              Array("5", "master2:2181,slave1:2181,slave2:2181", "mytest", "baotest", "1", "hdfs://master1:8020/user/root/bao/test")
    val conf = new SparkConf()
        .setMaster("spark://master2:7077")
        .setAppName("Streaming Test")
        .set("spark.streaming.receiver.writeAheadLog.enable", "true")
    val ssc = new StreamingContext(conf, Seconds(ssTime.toInt))

    val arrTopic = topics.split(",")
    arrTopic.foreach(etpc => {
      ssc.checkpoint("hdfs://master1:8020/user/root/bao/checkpoint/" + etpc + "/")
      val topicMap = Map[String, Int]((etpc, numThreads.toInt))
      val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

      println(lines.count)

//      lines.foreachRDD(lrdd => lrdd.repartition(1).saveAsTextFile(strPath + "/" + etpc + "/" + timename + "/"))
//      lines.saveAsTextFiles(strPath + "/" + etpc + "/" + scala.math.random + "/")
    })

    ssc.start()
    ssc.awaitTermination()

  }
}

