package com.practice.git_master

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum}

object _3Aggregations {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Aggregation")
      .master("local")
      .getOrCreate()

    val parseOption = Map("inferSchema" -> "true","header"->"true")
    val customerTransact = spark.read.format("csv").options(parseOption).load("/home/hduser/IdeaProjects/SparkPractice1/src/main/resources/datasets/transactions.csv")

    customerTransact.show()
    import spark.implicits._

    customerTransact.select('cc_num,'amt).groupBy('cc_num).agg(sum('amt).as("total")).orderBy('total desc).show()

    println(customerTransact.select('merchant).distinct().count())
  }
}
