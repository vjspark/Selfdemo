package com.kouga.scala.wordcount

import org.apache.spark.sql.SparkSession

/**
  * Created by dd on 2017/1/10.
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("wordcount")
      .getOrCreate()

    import sparkSession.implicits._

    val data = sparkSession.read.text("src/main/resources/word.txt").as[String]
    val words = data.flatMap(value => value.split("\\s+"))
    val groupedWords = words.groupByKey(_.toLowerCase)
    val counts = groupedWords.count()
    counts.show()
  }
}
