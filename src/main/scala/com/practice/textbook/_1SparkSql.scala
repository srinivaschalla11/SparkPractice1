package com.practice.textbook

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, column}
import org.apache.spark.sql.types._

object _1SparkSql {

  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("spark-sql")
      .getOrCreate()

    val df = spark.read.option("inferSchema","true")
      .csv("/home/hduser/data_sets/family.csv")

    val df1 = spark.read.format("csv").option("inferSchema","true").load("/home/hduser/data_sets/family.csv")

    val sch = StructType(
        StructField("id",IntegerType,true)
      ::StructField("name",StringType,true)
      ::StructField("salary",DoubleType,true)
      ::Nil)

     import spark.implicits._

     val df3 = spark.read.schema(sch).csv("/home/hduser/data_sets/family.csv")

     df3.select(col("id")).show()
     df3.select(column("id")).show()
     df3.select($"id").show()
     df3.select('id).show()


     df3.select(col("id")).show()
    df3.select("name").show()
    df3.select($"salary").show()
    df3.select('id).head()

    val myRow = Row(1,"srinivas",20.8)

    println(myRow(1));

    println(myRow(0).asInstanceOf[Int])


  }

}
