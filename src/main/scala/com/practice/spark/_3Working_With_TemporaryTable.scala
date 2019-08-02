package com.practice.spark

import org.apache.spark.sql.SparkSession

object _3Working_With_TemporaryTable {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Working with Temporary Table")
      .master("local")
      .getOrCreate()

    val parquetDF = spark.read.parquet("/home/hduser/data_sets/userdata1.parquet")
    parquetDF.registerTempTable("my_temporary_table")

    val tempTableDF = spark.sql("select * from my_temporary_table where country = 'China' ")
    tempTableDF.show()

  }

}
