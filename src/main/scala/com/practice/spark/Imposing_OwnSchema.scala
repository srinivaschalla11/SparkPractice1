package com.practice.spark

//import org.apache.calcite.avatica.ColumnMetaData.StructType
import org.apache.spark.sql.types._
import org.apache.spark.sql.{SparkSession, types}


object Imposing_OwnSchema {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Imposing_OwnSchema")
      .master("local")
      .getOrCreate()

    val defaultSchemaDF = spark.read.csv("/home/hduser/data_sets/sample.csv")
    defaultSchemaDF.printSchema()
    defaultSchemaDF.show()

    val myschema = StructType(
      StructField("ID",IntegerType,true)::
      StructField("NAME",StringType,true)::
      StructField("SALARY",LongType,true):: Nil
    )

    val namesWithOwnSchema = spark.read
      .option("header",true)
      .schema(myschema)
      .csv("/home/hduser/data_sets/family.csv")
    println("******************************************")

    namesWithOwnSchema.printSchema()
    namesWithOwnSchema.show()
  }

}
