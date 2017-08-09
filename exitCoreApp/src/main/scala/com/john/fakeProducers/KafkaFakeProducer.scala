package com.john.fakeProducers

import java.util.concurrent.Future

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

import scala.io.Source

/**
  * Created by jonathasalves on 26/06/2017.
  */
object KafkaFakeProducer {

  //val topic = util.Try(args(0)).getOrElse("test")
  val topic = "exit_dma_topic"
  val file = "/Users/jonathasalves/Programming/nasa_access_log"

  def main(args: Array[String]): Unit = {
    println(s"Connecting to topic: $topic")
    while (true) {
      val iterator = readFiles(file)
      pushData(iterator)
      Thread.sleep(3000)
    }
  }

  //  val props = new java.util.Properties()
  //  props.put("bootstrap.servers", "localhost:9092")
  //  props.put("client.id", "KafkaProducer")
  //  props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
  //  props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
  //
  //  val producer = new KafkaProducer[Integer, String](props)
  //
  //  import org.apache.kafka.clients.producer.ProducerRecord
  //
  //  val polish = java.time.format.DateTimeFormatter.ofPattern("dd.MM.yyyy H:mm:ss")
  //
  //  val lines = readFiles(file)
  //  var n = 3
  //  while(n < 100000) {
  //    val now = java.time.LocalDateTime.now().format(polish)
  //    val record = new ProducerRecord[Integer, String](topic, 1, n.toString)
  //    val metaF: Future[RecordMetadata] = producer.send(record)
  //    val meta = metaF.get()
  //    // blocking!
  //    val msgLog =
  //    s"""|offset    = ${meta.offset()}|partition = ${meta.partition()}|topic     = ${meta.topic()}""".stripMargin
  //    println(msgLog)
  //    Thread.sleep(10)
  //    n += 3
  //  }
  //
  //  producer.close()

  def readFiles(file: String): Iterator[String] = {
    for (line <- Source.fromFile(file).getLines()) yield line
  }

  def pushData(listData: Iterator[String]) = {

    val props = new java.util.Properties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("client.id", "KafkaProducer")
    props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[Integer, String](props)

    for (line <- listData) {
      val record = new ProducerRecord[Integer, String](topic, 1, line)
      val metaF: Future[RecordMetadata] = producer.send(record)
      val meta = metaF.get()
      // blocking!
      val msgLog =
        s"""|offset    = ${meta.offset()}|partition = ${meta.partition()}|topic     = ${meta.topic()}""".stripMargin
      println(msgLog)
      Thread.sleep(2000)
    }

    producer.close()
  }
}
