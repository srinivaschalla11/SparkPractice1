package com.practice.spark

import org.apache.spark.sql.SparkSession

object _4Working_With_Temporary_Table2 {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Working with temporary table with 2.X")
      .master("local")
      .getOrCreate()

    val parquetDF = spark.read.parquet("/home/hduser/data_sets/userdata1.parquet");
    parquetDF.createTempView("customer_info")

    /*
    * Here customer_info table is already exists if we create another customer_info temporary table with
    * createTempView() it gives error like customer_info already exists.
    * In some senerios we want to overwrite existing table in that case we will use
    * createOrReplaceTempView(). It will overwrite temporary view which is already present.
    * */

    val customerDF= spark.sql("select * from customer_info where gender = 'Male'")
    customerDF.createOrReplaceTempView("customer_info")
    spark.sql("select * from customer_info").show()
  }

}
