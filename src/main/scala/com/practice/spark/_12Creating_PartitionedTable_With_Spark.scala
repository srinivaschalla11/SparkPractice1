package com.practice.spark

import org.apache.spark.sql.{SaveMode, SparkSession}

object _12Creating_PartitionedTable_With_Spark {

  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("creating partitioned table in Hive throug spark")
      .enableHiveSupport()
      .getOrCreate()

    val df = spark.read.option("header","true")
      .option("inferSchema","true")
      .csv("/home/hduser/data_sets/nse-stocks-data.csv")

    df.write
        .partitionBy("series")
      .mode(SaveMode.Append)
      .saveAsTable("mydb.newpartition")

  }

}
