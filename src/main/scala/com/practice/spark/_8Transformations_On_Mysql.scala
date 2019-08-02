package com.practice.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession


object _8Transformations_On_Mysql {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Transformations on mysql table")
      .master("local")
      .getOrCreate()

    val url = "jdbc:mysql://localhost:3306"
    val table = "retail_db.customers"
    val properties = new Properties();
    properties.put("user","root")
    properties.put("password","root")

    //Class.forName("com.mysql.jdbc.driver")

    val mysqlDF = spark.read.jdbc(url,table,properties)
    mysqlDF.select("customer_state").groupBy("customer_state").count().show()


  }
}
