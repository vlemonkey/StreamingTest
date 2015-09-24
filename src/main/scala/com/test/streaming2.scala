package com.test

import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}

import org.apache.spark.sql.{SaveMode, SQLContext}


/**
 * Created by s on 15-6-4.
 */
object SQLContextSingleton {
  @transient private var instance: SQLContext = null

  // Instantiate SQLContext on demand
  def getInstance(sparkContext: SparkContext): SQLContext = synchronized {
    if (instance == null) {
      instance = new SQLContext(sparkContext)
    }
    instance
  }
}

object Consumer {
  def main (args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: KafkaWordCount <zkQuorum> <group> <topics> <numThreads>")
      System.exit(1)
    }

    val Array(zkQuorum, group, topics, numThreads) = args
    val conf = new SparkConf().setAppName("Streaming Test")
    val ssc = new StreamingContext(conf, Seconds(20))
//    ssc.checkpoint("checkpoint1")

    val schemaString =
      """
        |AUID,
        |MD5_CODE,
        |PROVINCE_ID,
        |CITY_ID,
        |AREA_ID,
        |TIME_BEGIN,
        |TIME_END,
        |MSISDN,
        |IMSI,
        |IMEI,
        |NETWORK_TYPE,
        |DEVICE_NAME,
        |DEVICE_TYPE,
        |SOFT_VER,
        |SERVICE_TYPE,
        |TEST_CASE,
        |LATITUDE,
        |LONGITUDE,
        |TEST_ADDR,
        |MAIN_CELL,
        |LOG_PATH,
        |LOG_UFILE,
        |LOG_PFILE,
        |LOG_SIZE,
        |GPS_DISTANCE,
        |DURATION,
        |LOG_STATUS,
        |LOG_FORSEARCH,
        |PHOTO_MD5,
        |LOG_DATE,
        |WORK_ID,
        |ITEMS_ID,
        |LOG_QUALITY,
        |INVALID_CAUSE,
        |LOG_TYPE,
        |TEST_TYPE,
        |TEST_SCENE,
        |NODE_ID,
        |AUID2,
        |MD5_CODE2,
        |PROVINCE_ID2,
        |CITY_ID2,
        |AREA_ID2,
        |TIME_BEGIN2,
        |TIME_END2,
        |MSISDN2,
        |IMSI2,
        |IMEI2,
        |NETWORK_TYPE2,
        |DEVICE_NAME2,
        |DEVICE_TYPE2,
        |SOFT_VER2,
        |SERVICE_TYPE2,
        |TEST_CASE2,
        |LATITUDE2,
        |LONGITUDE2,
        |TEST_ADDR2,
        |MAIN_CELL2,
        |LOG_PATH2,
        |LOG_UFILE2,
        |LOG_PFILE2,
        |LOG_SIZE2,
        |GPS_DISTANCE2,
        |DURATION2,
        |LOG_STATUS2,
        |LOG_FORSEARCH2,
        |PHOTO_MD52,
        |LOG_DATE2,
        |WORK_ID2,
        |ITEMS_ID2,
        |LOG_QUALITY2,
        |INVALID_CAUSE2,
        |LOG_TYPE2,
        |TEST_TYPE2,
        |TEST_SCENE2,
        |NODE_ID2
      """.stripMargin

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, FloatType}

    val schema =
      StructType(
        Array(StructField("AUID", IntegerType, true),
          StructField("MD5_CODE", StringType, true),
          StructField("PROVINCE_ID", IntegerType, true),
          StructField("CITY_ID", IntegerType, true),
          StructField("AREA_ID", IntegerType, true),
          StructField("TIME_BEGIN", IntegerType, true),
          StructField("TIME_END", IntegerType, true),
          StructField("MSISDN", IntegerType, true),
          StructField("IMSI", IntegerType, true),
          StructField("IMEI", StringType, true),
          StructField("NETWORK_TYPE", IntegerType, true),
          StructField("DEVICE_NAME", StringType, true),
          StructField("DEVICE_TYPE", IntegerType, true),
          StructField("SOFT_VER", StringType, true),
          StructField("SERVICE_TYPE", IntegerType, true),
          StructField("TEST_CASE", IntegerType, true),
          StructField("LATITUDE", FloatType, true),
          StructField("LONGITUDE", FloatType, true),
          StructField("TEST_ADDR", StringType, true),
          StructField("MAIN_CELL", IntegerType, true),
          StructField("LOG_PATH", StringType, true),
          StructField("LOG_UFILE", StringType, true),
          StructField("LOG_PFILE", StringType, true),
          StructField("LOG_SIZE", IntegerType, true),
          StructField("GPS_DISTANCE", IntegerType, true),
          StructField("DURATION", IntegerType, true),
          StructField("LOG_STATUS", IntegerType, true),
          StructField("LOG_FORSEARCH", StringType, true),
          StructField("PHOTO_MD5", StringType, true),
          StructField("LOG_DATE", IntegerType, true),
          StructField("WORK_ID", IntegerType, true),
          StructField("ITEMS_ID", IntegerType, true),
          StructField("LOG_QUALITY", IntegerType, true),
          StructField("INVALID_CAUSE", StringType, true),
          StructField("LOG_TYPE", IntegerType, true),
          StructField("TEST_TYPE", IntegerType, true),
          StructField("TEST_SCENE", IntegerType, true),
          StructField("NODE_ID", IntegerType, true),
          StructField("AUID2", IntegerType, true),
          StructField("MD5_CODE2", StringType, true),
          StructField("PROVINCE_ID2", IntegerType, true),
          StructField("CITY_ID2", IntegerType, true),
          StructField("AREA_ID2", IntegerType, true),
          StructField("TIME_BEGIN2", IntegerType, true),
          StructField("TIME_END2", IntegerType, true),
          StructField("MSISDN2", IntegerType, true),
          StructField("IMSI2", IntegerType, true),
          StructField("IMEI2", StringType, true),
          StructField("NETWORK_TYPE2", IntegerType, true),
          StructField("DEVICE_NAME2", StringType, true),
          StructField("DEVICE_TYPE2", IntegerType, true),
          StructField("SOFT_VER2", StringType, true),
          StructField("SERVICE_TYPE2", IntegerType, true),
          StructField("TEST_CASE2", IntegerType, true),
          StructField("LATITUDE2", FloatType, true),
          StructField("LONGITUDE2", FloatType, true),
          StructField("TEST_ADDR2", StringType, true),
          StructField("MAIN_CELL2", IntegerType, true),
          StructField("LOG_PATH2", StringType, true),
          StructField("LOG_UFILE2", StringType, true),
          StructField("LOG_PFILE2", StringType, true),
          StructField("LOG_SIZE2", IntegerType, true),
          StructField("GPS_DISTANCE2", IntegerType, true),
          StructField("DURATION2", IntegerType, true),
          StructField("LOG_STATUS2", IntegerType, true),
          StructField("LOG_FORSEARCH2", StringType, true),
          StructField("PHOTO_MD52", StringType, true),
          StructField("LOG_DATE2", IntegerType, true),
          StructField("WORK_ID2", IntegerType, true),
          StructField("ITEMS_ID2", IntegerType, true),
          StructField("LOG_QUALITY2", IntegerType, true),
          StructField("INVALID_CAUSE2", StringType, true),
          StructField("LOG_TYPE2", IntegerType, true),
          StructField("TEST_TYPE2", IntegerType, true),
          StructField("TEST_SCENE2", IntegerType, true),
          StructField("NODE_ID2", IntegerType, true)
        )
      )


    val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
    val lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)
    print("\n\n" + lines.count + "\n\n")

    lines.foreachRDD { rdd =>
      val sqlContext = SQLContextSingleton.getInstance(rdd.sparkContext)

      val rowRDD = rdd.map(_.split(",")).map(p => Row(
        p(0).trim.toInt,
        p(1).trim,
        p(2).trim.toInt,
        p(3).trim.toInt,
        p(4).trim.toInt,
        p(5).trim.toInt,
        p(6).trim.toInt,
        p(7).trim.toInt,
        p(8).trim.toInt,
        p(9).trim,
        p(10).trim.toInt,
        p(11).trim,
        p(12).trim.toInt,
        p(13).trim,
        p(14).trim.toInt,
        p(15).trim.toInt,
        p(16).trim.toFloat,
        p(17).trim.toFloat,
        p(18).trim,
        p(19).trim.toInt,
        p(20).trim,
        p(21).trim,
        p(22).trim,
        p(23).trim.toInt,
        p(24).trim.toInt,
        p(25).trim.toInt,
        p(26).trim.toInt,
        p(27).trim,
        p(28).trim,
        p(29).trim.toInt,
        p(30).trim.toInt,
        p(31).trim.toInt,
        p(32).trim.toInt,
        p(33).trim,
        p(34).trim.toInt,
        p(35).trim.toInt,
        p(36).trim.toInt,
        p(37).trim.toInt,

        p(38).trim.toInt,
        p(39).trim,
        p(40).trim.toInt,
        p(41).trim.toInt,
        p(42).trim.toInt,
        p(43).trim.toInt,
        p(44).trim.toInt,
        p(45).trim.toInt,
        p(46).trim.toInt,
        p(47).trim,
        p(48).trim.toInt,
        p(49).trim,
        p(50).trim.toInt,
        p(51).trim,
        p(52).trim.toInt,
        p(53).trim.toInt,
        p(54).trim.toFloat,
        p(55).trim.toFloat,
        p(56).trim,
        p(57).trim.toInt,
        p(58).trim,
        p(59).trim,
        p(60).trim,
        p(61).trim.toInt,
        p(62).trim.toInt,
        p(63).trim.toInt,
        p(64).trim.toInt,
        p(65).trim,
        p(66).trim,
        p(67).trim.toInt,
        p(68).trim.toInt,
        p(69).trim.toInt,
        p(70).trim.toInt,
        p(71).trim,
        p(72).trim.toInt,
        p(73).trim.toInt,
        p(74).trim.toInt,
        p(75).trim.toInt
      ))

      val df = sqlContext.createDataFrame(rowRDD, schema)
      df.registerTempTable("test")

      val results = sqlContext.sql("select AUID, IMSI, SUM(LOG_SIZE) from test GROUP BY AUID, IMSI")

//      results.save("bao/kafka/output", SaveMode.Append)

      results.save("bao/kafka/output")
    }

    ssc.start()
    ssc.awaitTermination()

  }
}


// Produces some random words between 1 and 100
/**
 * `spark-submit --master spark://master2:7077 --class com.test.Producer streamingtest_2.10-1.0.jar  slave2:9092,master2:9092 test2 100 100`
 */
object Producer {
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
        val str = "1,sdfehfu,7,100,101,1432949450,1433305965,3,12345,xxx,2,lenovo,3,nnn,1,1,12.78,34.89,shangdi,6," +
          "/var/log,rizhi.txt,mail,125555,90,5678,1,hhkkk,djafdjfn,1433310369,5,8,1,cause,1,2,12,9," +
          "1,sdfehfu,7,100,101,1432949450,1433305965,3,12345,xxx,2,lenovo,3,nnn,1,1,12.78,34.89,shangdi,6," +
          "/var/log,rizhi.txt,mail,125555,90,5678,1,hhkkk,djafdjfn,1433310369,5,8,1,cause,1,2,12,9"
        new KeyedMessage[String, String](topic, str)
      }.toArray

      producer.send(messages: _*)
      Thread.sleep(100)
    }

  }
}
