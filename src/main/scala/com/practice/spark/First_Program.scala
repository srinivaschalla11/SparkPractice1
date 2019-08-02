package com.practice.spark


import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext, sql}

object First_Program {

  def main(args:Array[String]):Unit={
   val sparkSession = SparkSession.builder()
     .appName("My_first")
     .master("local")
     .getOrCreate()

    var array = Array(1,2,3,4,5,6)

    val arrayRDD = sparkSession.sparkContext.parallelize(array,2)

    arrayRDD.foreach(println)



  }
}
