package com.practice.spark

import org.apache.spark.sql.SparkSession

object _6DataSets_Basic_Operations {

  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder()
      .appName("DataSet_Basic_Operations")
      .master("local")
      .getOrCreate()

    //  val csvDS = spark.read.csv("/home/hduser/data_sets/family.csv").as[]

    ()
  }

}
