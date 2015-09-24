package com.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by s on 15-7-31.
 */
object steamingtest4 {
    def main(args: Array[String]) {

      val zkQuorum="slave2:8121"
      val group="consumer-group"
      val topics="tt"
      val numThreads="1"

      val sparkConf = new SparkConf().setAppName("KafkaWordCount").setMaster("spark://master2:7077")
        .set("spark.executor.cores", "1")
      val ssc =  new StreamingContext(sparkConf, Seconds(5))
//      ssc.checkpoint("hdfs://cdh5cluster:8020/liulei")

      val topicMap = topics.split(",").map((_,numThreads.toInt)).toMap
      val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
      println("########################################")
//      println(lines.count())

      lines.foreachRDD{ iter =>
        print("------->")
        iter.foreach(println)
      }

//      lines.foreach( line => {
//        println("---------------->line.count="+line.count)
//      })
//
//
//      val words = lines.flatMap(_.split(" "))
//
//
//      words.foreach( word => {
//        println("--------------->word.count="+word.count)
//      })


      ssc.start()
      ssc.awaitTermination()
    }
}
