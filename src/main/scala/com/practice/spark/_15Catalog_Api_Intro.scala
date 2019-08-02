package com.practice.spark

import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.StructField


object _15Catalog_Api_Intro {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("catalog_api")
      .master("local")
      .enableHiveSupport()
      .getOrCreate();

    val catalog = spark.catalog

    /*
     * 1. name of the columns
     * 2. data type of the column
     * 3. nullable
     */

    val movies = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("datasets/movielens/movie.csv")

    val schema = StructType(
      StructField("movieId", IntegerType, false)
        :: StructField("title", StringType, true)
        :: StructField("genres", StringType, true)
        :: Nil)

    catalog.createTable("spark_course.movies", "parquet", schema, Map("Comments" -> "Table Created with Catalog API"))

    println("Table Successfully Created: " + catalog.tableExists("spark_course.movies"))

    movies.write.insertInto("spark_course.movies")

    spark.table("spark_course.movies").show()

  }


}
