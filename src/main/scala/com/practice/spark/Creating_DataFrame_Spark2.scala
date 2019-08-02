package com.practice.spark


import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.GetArrayStructFields
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object Creating_DataFrame_Spark2 {

  def main(args:Array[String]):Unit = {
/*    In spark 1.x first we will create SparkConf object and later SparkContext and with this object we will create
      SqlContext or HiveContext objects to create DataFrame, but in Spark 2x we will create by SparkSession object like
      show blow
    kjfk*/

    val spark = SparkSession.builder()
      .appName("Creating RDD Using Sprak 2")
      .master("local")
      .getOrCreate()

    val arr = Array(1,2,3,4,5,6,7,8)
    val rdd = spark.sparkContext.parallelize(arr)


    val schema = StructType(
      StructField("Numbers",IntegerType,false)
       :: Nil
    )

    val rddRow = rdd.map(ele => Row(ele))

    val dataframe = spark.createDataFrame(rddRow,schema )

    dataframe.printSchema();
    println("******************************************")

    dataframe.show()

  }


}
