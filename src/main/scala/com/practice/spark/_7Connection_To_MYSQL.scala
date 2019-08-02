package com.practice.spark

import java.util.Properties

import org.apache.spark.sql.SparkSession

object _7Connection_To_MYSQL {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("connecting to mysql_DB")
      .master("local")
      .getOrCreate();

    val url = "jdbc:mysql://localhost:3306"
    val table = "retail_db.customers"
    val properties = new Properties()
    properties.put("user","root")
    properties.put("password","root")
    /*
    * To connect to database we will use jdbc(). In this jdbc() we will pass below
    * properties. If any error regarding cant access database. We should grant permissions
    * to root user.
    * */

    val mysqlDF = spark.read.jdbc(url,table,properties)
    mysqlDF.show()
  }
}
