package com.practice.spark

import java.io.File

import org.apache.spark.sql.{SaveMode, SparkSession}

object _11Writing_DataFrame_Into_Hive {

  def main(args:Array[String]) = {

    //val warehouseLocation = "hdfs://localhost:9000/user/hive/warehouse/"

    val spark = SparkSession.builder()
      .appName("Loading DataFrame into Hive table")
      .master("local")
      .enableHiveSupport()
     // .config("livy.repl.enableHiveContext",true)
     // .config("spark.sql.warehouse.dir", warehouseLocation)
     // .enableHiveSupport()
      .getOrCreate()

   //val df = spark.sql("select * from mydb.employee")

   // val df2 = spark.sqlContext.sql("select * from mydb.employee")


  val df = spark.read
      .option("header","true")
      .option("inferSchema","true")
      .csv("/home/hduser/data_sets/nse-stocks-data.csv")

    //spark.sql("select  ")

    df.write
      .mode(SaveMode.Append)
      .saveAsTable("mydb.nyse")

    df.show()



  }

}
