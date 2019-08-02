package com.practice.spark

import org.apache.spark.sql.SparkSession
/*
*  For working with avro file in spark 2.x there is no
*  default library we need to import from maven repositroy and
*  include in our pom file and work like show below
 */

object Avro_File_Example {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Working with Avro File")
      .master("local")
      .getOrCreate()

    val avroDF = spark.read
      .format("com.databricks.spark.avro")
      .option("header",false)
      .option("inferSchema",true)
      .load("/home/hduser/data_sets/userdata1.avro")

    avroDF.printSchema();
    avroDF.show(6)

    avroDF.write.format("com.databricks.spark.avro").save("/home/hduser/data_sets/output_avro")
  }

}
