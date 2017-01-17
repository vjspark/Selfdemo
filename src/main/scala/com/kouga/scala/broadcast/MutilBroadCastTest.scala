package com.kouga.scala.broadcast

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Created by dd on 2017/1/17.
  */
object MutilBroadCastTest {
  def main(args : Array[String]): Unit = {
    val sparkSession = SparkSession
      .builder
      .master("local")
      .appName("MutilBroadCastTestForScala")
      .getOrCreate()

    val slices = if (args.length > 0) args(0).toInt else  2
    val num = if (args.length > 1) args(1).toInt else 1000000

    val arr1 = new Array[Int](num)
    for (i <- 0 until arr1.length) {
      arr1(i) = i
    }

    val arr2 = new Array[Int](num)
    for (i <- 0 until arr2.length) {
      arr2(i) = i
    }

    var barr1 = sparkSession.sparkContext.broadcast(arr1)
    val barr2 = sparkSession.sparkContext.broadcast(arr2)
    val observedSizes : RDD[(Int,Int)] = sparkSession.sparkContext.parallelize(1 to 10, slices).map{
      _ => (barr1.value.length, barr2.value.length)
    }

    observedSizes.collect().foreach(i => println(i))

    sparkSession.stop()
  }
}
