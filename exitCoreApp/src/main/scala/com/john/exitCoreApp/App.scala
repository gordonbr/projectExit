package com.john.exitCoreApp

import java.util.concurrent.Future
import java.util.regex.Pattern

import kafka.serializer.StringDecoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.{SparkConf, SparkContext}

class IngesterParams(val topicsSet: Set[String], val brokers: String)

/**
  * @author Jonathas Alves
  */
object App {

  def loadURL(path: String, sc: SparkContext): RDD[String] = {
    sc.textFile(path)
  }

  def splitURL(urlsRDD: DStream[String]): DStream[(String, String, String, String, String)] = {
    val regEx: String = "^(\\d{1,3}\\.){3}\\d{1,3} - - \\[(\\d){2}/(\\w){3}/(\\d){4}:(\\d){2}:(\\d){2}:(\\d){2} -0400\\]" +
      " (\".+\") (\\d)+ (\\d)+?"

    val pattern = Pattern.compile("([\\S\\.]+) - - (\\[.*?\\]) (\".+\") (\\d+) (\\d+)")

    println("here here")

    val groupsRDD = urlsRDD.map(line => pattern.matcher(line)).filter(pat => pat.find()).map(pat =>
      (pat.group(1), pat.group(2), pat.group(3), pat.group(4), pat.group(5)))

    groupsRDD
  }

  def kafkaStream(ssc: StreamingContext, ingesterParams: IngesterParams) = {

    val kafkaParams = Map[String, String]("metadata.broker.list" -> ingesterParams.brokers, "auto.offset.reset" -> "smallest")
    val directKafkaStream = KafkaUtils.createDirectStream[
      String, String, StringDecoder, StringDecoder](ssc, kafkaParams, ingesterParams.topicsSet)

    val lines = directKafkaStream.map(_._2)
    lines.count().print()
    val groupsRDD = splitURL(lines)

    val stringStreamRDD = groupsRDD.map(groups =>
      String.format("%s == %s == %s == %s == %s", groups._1, groups._2, groups._3, groups._4, groups._5))

    stringStreamRDD.foreachRDD(rdd => rdd.collect().foreach(println))

    sparkToKafka(stringStreamRDD)
  }

  def sparkToKafka(stream: DStream[String]) = {

    stream.foreachRDD(rdd => {

      rdd.foreachPartition(partition => {
        val topic = "exit_processed_topic"

        val props = new java.util.Properties()
        props.put("bootstrap.servers", "localhost:9092")
        props.put("client.id", "KafkaProducer")
        props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[Integer, String](props)

        partition.foreach(line => {

          val record = new ProducerRecord[Integer, String](topic, 1, line.toString)
          producer.send(record)

        })

        producer.close()
      })
    })

  }

  def defSsc(ingesterParams: IngesterParams): StreamingContext = {

    val conf = new SparkConf().setAppName("simple-ingester").setMaster("local[*]")
    val ssc = new StreamingContext(conf, Seconds(1))
    kafkaStream(ssc, ingesterParams)
    ssc
  }

  def main(args: Array[String]) {

    println("it's alive spark")
    val ingesterParams = new IngesterParams(Set("exit_dma_topic"), "localhost:9092")
    val ssc = defSsc(ingesterParams)
    ssc.start()
    ssc.awaitTermination()
    ssc.stop()
  }


  def sparkToKafka2() = {

    val topic = "exit_processed_topic"

    val props = new java.util.Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")


    val producer = new KafkaProducer[Integer, String](props)
    val record = new ProducerRecord[Integer, String](topic, 1, "test")
    val metaF: Future[RecordMetadata] = producer.send(record)
    val meta = metaF.get()
    // blocking!
    val msgLog =
      s"""|offset    = ${meta.offset()}|partition = ${meta.partition()}|topic     = ${meta.topic()}""".stripMargin
    println(msgLog)
    Thread.sleep(5)
    producer.close()

  }

}
