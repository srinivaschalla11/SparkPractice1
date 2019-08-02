package com.practice.git_master

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col,explode,when,array,lit}

object _4Business {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("business")
      .master("local")
      .getOrCreate()

   val business =  spark.read.json("/home/hduser/IdeaProjects/SparkPractice1/src/main/resources/datasets/transactions.json")
    business.show()
    import spark.implicits._
    business.withColumn("category", when(col("category").
      notEqual("personal_care"),"Hello_World")
      .otherwise(lit("tupaki").cast("string"))).show()
  }

}
