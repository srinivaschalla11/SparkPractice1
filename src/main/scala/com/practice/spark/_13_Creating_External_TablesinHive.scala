package com.practice.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object _13_Creating_External_TablesinHive {

  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder()
      .appName("local")
      .master("local")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/home/hduser/data_sets/nse-stocks-data.csv")

    df.write
      .mode(SaveMode.Overwrite)
      .option("path","/user/hive/warehouse/external_table")
      .partitionBy("series")
      .saveAsTable("mydb.externaltable")


  }

}
