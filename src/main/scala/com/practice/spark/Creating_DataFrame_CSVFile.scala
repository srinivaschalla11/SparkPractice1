package com.practice.spark

import org.apache.spark.sql.SparkSession

object Creating_DataFrame_CSVFile {

  def main(args:Array[String]):Unit={

    val spark = SparkSession.builder()
      .appName("Creating_DataFrame_With_CSV")
      .master("local")
      .getOrCreate()

    val dataframe = spark.read
       /* .option("header",true)
      .option("inferSchema",true)*/
        .options(Map("header" -> "true","inferSchema" -> "true"))
      .csv("src/main/resources/datasets/sample.csv")

    dataframe.printSchema()
    dataframe.show()


  }

}
