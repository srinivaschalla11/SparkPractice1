package com.practice.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

object _9Query_Push_Down_to_MYSQL {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Quering mysql")
      .master("local")
      .getOrCreate()
    val url = "jdbc:mysql://localhost:3306"
   // val table = "retail_db.customers"
    val properties = new Properties();
    properties.put("user","root")
    properties.put("password","root")

    val query = "select * from retail_db.customers where customer_fname = 'Mary' ";

    val queryResult = spark.read.jdbc(url,s"($query) as temp",properties)
    queryResult.show(5)

  }
}
