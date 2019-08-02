package com.practice.textbook

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.lit

object _2CreatingDataFrame {

  def main(args:Array[String]):Unit = {
      val spark = SparkSession.builder
        .appName("creating_DataFrame")
        .master("local")
        .getOrCreate()

      val df = spark.read.format("json").load("/home/hduser/data_sets/students.json")
      df.show();

      df.createOrReplaceTempView("cricket")
      spark.sql("select * from cricket where name = 'aimee Zank'").show()

      val myRows = Seq(Row(1,"srinivas",4000),Row(2,"Spark",5000),Row(3,"Hive",9000))
      val rdd = spark.sparkContext.parallelize(myRows)

      val sch = StructType(StructField("id",IntegerType,false)::
                           StructField("name",StringType,false)::
                           StructField("salary",IntegerType,false)::Nil)
      val new_df = spark.createDataFrame(rdd,sch)

      new_df.select("id","name","salary").show()

      new_df.withColumn("number_one",lit(1)).show()
      new_df.withColumn("number_tow",lit(2)).show()

      new_df.withColumnRenamed("salary","sal").show()

      new_df.withColumn("`hello world-yes`",lit(1)).show()

    }

}
