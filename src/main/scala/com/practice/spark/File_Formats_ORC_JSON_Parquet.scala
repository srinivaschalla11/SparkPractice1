package com.practice.spark
import org.apache.spark.sql.SparkSession

object File_Formats_ORC_JSON_Parquet {

  /*
  *     If working with Spark 2.X and higher support:
  *     CSV,ORC,JSON,PARQUET file formats
  *
  *     If working with Spark1.x supports:
  *     JSON,ORC,PARQUET
   */

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Working with different file formats")
      .master("local")
      .getOrCreate()

    val jsonDF = spark.read.json("/home/hduser/data_sets/students.json")

    jsonDF.printSchema();
    jsonDF.show()
    println("count is "+jsonDF.count)

/*
    val orcDF = spark.read.orc("/home/hduser/data_sets/userdata.orc")

    orcDF.printSchema();
    orcDF.show()
    println("count is "+orcDF.count)*/

    val parquetDF = spark.read.parquet("/home/hduser/data_sets/userdata1.parquet")

    parquetDF.printSchema()
    parquetDF.show()
    println("count of parquetDf "+parquetDF.count())


  }

}
