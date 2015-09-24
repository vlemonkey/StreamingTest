package com.test

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * Created by s on 15-6-15.
 */
object socketTest {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("sockect test")
    val ssc = new StreamingContext(conf, Seconds(5))

    val Array(master, port) = args
    val lines = ssc.socketTextStream(master, port.toInt)

    val wordcount = lines.flatMap(_.split("")).map((_, 1)).reduceByKey(_ + _)
    wordcount.print()

    ssc.start()
    ssc.awaitTermination()
  }
}
