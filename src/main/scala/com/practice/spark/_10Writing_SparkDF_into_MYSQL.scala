package com.practice.spark

import java.util.Properties

import org.apache.spark.sql.{SaveMode, SparkSession}

object _10Writing_SparkDF_into_MYSQL {
    def main(args:Array[String]):Unit = {

      val spark = SparkSession.builder()
        .appName("writing DF into mysql table")
        .master("local")
        .getOrCreate()

      val url = "jdbc:mysql://localhost:3306"
      val properites = new Properties();
      properites.put("user","root")
      properites.put("password","root")

      /*
      * In the below program we will read data from csv file and our dataframe contains data
      * than we will write this dataframe in to mysql table with help of write.jdbc()
      * this write.mode(SaveMode.Append) will append content of dataframe into database content.
      * We are having different options like Append,Overwrite,ErrorIfExists etc.
      * */
      val df = spark.read.option("header","true")
        .option("inferschema","true")
        .csv("/home/hduser/data_sets/sample.csv")

      val table = "retail_db.sample"

      df.write.mode(SaveMode.Append).jdbc(url,table,properites)

    }
}
