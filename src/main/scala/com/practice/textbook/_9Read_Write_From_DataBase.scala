package com.practice.textbook

import org.apache.spark.sql.SparkSession

object _9Read_Write_From_DataBase {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("DataBase")
      .getOrCreate()


  }
}
