package com.practice.spark

import org.apache.spark.sql.SparkSession

object _1DataFrame_Basic_Operations {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("DataFrame Basic Operations")
      .master("local")
      .getOrCreate()

    val csvDF = spark.read.csv("/home/hduser/data_sets/family.csv")

    //to print schema
    csvDF.printSchema();

    //We have other method to print schema but it will print data in StructType.It is shown below
    val orcSchema = csvDF.schema
    println(orcSchema)

   /* IF we want to know only columns of DataFrame
     we have method called "columns" it returns all columns. It will return an array
    to print array elements we use mkString(",")*/
    val csvColumns = csvDF.columns
    println(csvColumns.mkString(","))

    /*We can describe a column with describe()
    Inside describe("column") we can pass column name as shown below*/
    /*val csvDescription = csvDF.describe("ID")
    csvDescription.show()*/

    csvDF.head(3).foreach(println)


  }

}
