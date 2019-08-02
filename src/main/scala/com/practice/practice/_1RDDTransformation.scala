package com.practice.practice

import org.apache.spark.sql.SparkSession

object _1RDDTransformation {

  def main(args:Array[String])={
    val spark = SparkSession.builder()
      .master("local")
      .appName("first_rdd")
      .getOrCreate()

   // val familyrdd = spark.sparkContext.textFile("/home/hduser/data_sets/family.csv")
   val df = spark.read.format("csv").option("header","true")
     .option("inferSchema","true")
     .load("/home/hduser/data_sets/family.csv")

   val rdd1 = df.rdd.map(element => {
     val ele = element.toString();
     val arr = ele.split(",")
     (arr(1),arr(2))
   })

   val total = rdd1.reduceByKey(_+_)
    total.saveAsTextFile("/home/hduser/data_sets/samp.csv")


  }

}
