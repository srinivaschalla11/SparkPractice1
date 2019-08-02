package com.practice.textbook

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count,countDistinct,sum,avg,min,max}

object _5AggregationExamples {

  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder()
      .appName("Aggregation")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv").option("header","true")
      .option("inferSchema","true").load("/home/hduser/data_sets/family.csv").coalesce(5)

    df.cache()

    df.createOrReplaceTempView("family")

   // spark.sql("select * from family where id = 4").show()

    df.select(count("name")).show()
    df.select(countDistinct("name")).show()

    df.select(sum("id").alias("sum"),avg("id").alias("avg"),
      max("id"),min("id") ).show()

    df.groupBy("name").count().show()
    df.groupBy("name").agg(count("*").alias("count")).show()

    df.show();
    df.groupBy("name").agg(max("id")).show()

    //df.show()
  }

}
