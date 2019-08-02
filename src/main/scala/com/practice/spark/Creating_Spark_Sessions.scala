package com.practice.spark

import org.apache.spark.sql.SparkSession

object Creating_Spark_Sessions {

  def main(args:Array[String]):Unit = {
    val spark1 = SparkSession.builder()
      .appName("Creating_Spark_Sessions")
      .master("local")
      .getOrCreate()

    val spark2 = SparkSession.builder()
      .appName("Creating_Saprk_Sessions")
      .master("local")
      .getOrCreate()

    val arr1 = Array(1,2,3,4,5)
    val arr2 = Array(6,7,8,9)

    val rdd1 = spark1.sparkContext.parallelize(arr1)
    val rdd2 = spark2.sparkContext.parallelize(arr2)

    rdd1.collect.foreach(println)
    rdd2.collect.foreach(println)

  }

}
