package com.practice.git_master

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{sum,count,desc}

object _2Transac_Amount_Swipe_Per_Category {

  def main(args:Array[String]) ={
    val spark = SparkSession.builder()
      .appName("amount_per_category")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    val sch = Map("inferSchema" -> "true","header" -> "true")
    val transaction  = spark.read.format("csv").options(sch).load("/home/hduser/IdeaProjects/SparkPractice1/src/main/resources/datasets/transactions.csv")
    val groupedDF = transaction.groupBy('category)

    groupedDF.agg(sum("amt").as("total"),count("cc_num").as("count"))
      .orderBy(desc("total")).show()
  }

}
