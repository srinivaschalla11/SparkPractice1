package com.practice.spark

import org.apache.spark.sql.SparkSession

object _2DataFrame_Basic_Operations {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("2nd Basic DataFrame Operations")
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.json("/home/hduser/data_sets/students.json")
    //Usage of filter and where clause is shown below in DataFrame
    //similarly we can use groupBy()
    val selectedColumn = jsonDF.select("name","scores")
      //.where("name = 'aimee Zank'")
        .filter(jsonDF("name") === "aimee Zank")
      //.groupBy("column_name").count()
    selectedColumn.show()


  }

}
