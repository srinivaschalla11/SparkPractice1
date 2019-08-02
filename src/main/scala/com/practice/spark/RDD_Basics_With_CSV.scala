package com.practice.spark

import org.apache.spark.sql.SparkSession

object RDD_Basics_With_CSV {

  def main(args:Array[String]):Unit = {

     val sparkSession = SparkSession.builder()
       .appName("Creating_RDD_With_CSV_file")
       .master("local")
       .getOrCreate()

    val file = "src/main/resources/datasets/sample.csv"

    val csvRDD = sparkSession.sparkContext.textFile(file)
    val header = csvRDD.first()


    csvRDD.take(3).foreach(println)
    println("*******************************")

    //printing without header
    val csvRDDWithoutHeader = csvRDD.filter(_ != header)
    csvRDDWithoutHeader.take(2).foreach(println)

    //printing only first column
    val phraseid = csvRDD.map(ele =>
    {
      val elements = ele.split(",")
      (elements(0),elements(3),elements(4))
    }
    )

    phraseid.take(3).foreach(println)
    println("**********************************")

    val phraseid1= csvRDD.map(ele =>
    {
      val elements = ele.split(",")
      Array(elements(0),elements(3),elements(4)).mkString(",")
    }
    )

    phraseid1.take(3).foreach(println)

    phraseid1.saveAsTextFile("/home/hduser/Desktop/saveAsTextFileExample/");



  }

}
