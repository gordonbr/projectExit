package com.john.fakeProducers

import scala.io.Source

/**
  * Created by jonathasalves on 26/06/2017.
  */
object KafkaProducer {

  def main(args : Array[String]) = {
    print("And I`ll try to fix you")

  }

  def readFiles(file : String) : Iterator[String] = {
    for(line <- Source.fromFile(file).getLines()) yield line
  }
}
