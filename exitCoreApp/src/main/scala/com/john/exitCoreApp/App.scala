package com.john.exitCoreApp

import java.util.regex.Pattern

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author ${user.name}
 */
object App {

  def loadURL(path:String, sc:SparkContext) : RDD[String] = {
    sc.textFile(path)
  }
  
  def splitURL(urlsRDD:RDD[String]) = {
    val regEx: String = "^(\\d{1,3}\\.){3}\\d{1,3} - - \\[(\\d){2}/(\\w){3}/(\\d){4}:(\\d){2}:(\\d){2}:(\\d){2} -0400\\]" +
      " (\".+\") (\\d)+ (\\d)+?"

    val pattern = Pattern.compile("([\\S\\.]+) - - (\\[.*?\\]) (\".+\") (\\d+) (\\d+)")

    val groupsRDD = urlsRDD.map(line => pattern.matcher(line)).filter(pat => pat.find()).map(pat =>
      (pat.group(1), pat.group(2), pat.group(3), pat.group(4), pat.group(5)))

    //groupsRDD.collect().foreach(group => println(String.format("%s %s %s %s %s", group._1, group._2, group._3, group._4, group._5)))
    println("TOTAL COUNT " + groupsRDD.count())
  }


  def matchFunction() = {
    val text = "199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] \"GET /history/apollo/ HTTP/1.0\" 200 6245"
    val regEx: String = "^(\\d{1,3}\\.){3}\\d{1,3} - - \\[(\\d){2}/(\\w){3}/(\\d){4}:(\\d){2}:(\\d){2}:(\\d){2} -0400\\]" +
      " (\".+\") (\\d)+ (\\d)+?"
    val pattern = Pattern.compile("([\\d\\.]+) - - (\\[.*?\\]) (\".+\") (\\d+) (\\d+)")

    System.out.println(text.matches(regEx))
    val pat = pattern.matcher(text)
    if(pat.find())
      System.out.println(pat.group(1) + " " + pat.group(2) + " " + pat.group(3) + " " + pat.group(4) + " " + pat.group(5))
  }

  
  def main(args : Array[String]) {

    matchFunction()
    val sconf = new SparkConf().setMaster("local").setAppName("coreApp")
    val sc = new SparkContext(sconf)

    val urlRDD = loadURL("/Users/jonathasalves/Programming/nasa_access_log", sc)

    splitURL(urlRDD)
  }

}
