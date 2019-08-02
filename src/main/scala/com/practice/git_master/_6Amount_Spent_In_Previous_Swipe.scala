package com.practice.git_master

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{lag}

object _6Amount_Spent_In_Previous_Swipe {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("perivious swipe")
      .master("local")
      .getOrCreate()

    val customerDF = spark.read.format("csv").option("InferSchema","true")
      .option("header","true").load("/home/hduser/IdeaProjects/SparkPractice1/src/main/resources/datasets/customers.csv")

    val transactionDF = spark.read.format("csv").option("inferSchema","true")
      .option("header","true").load("/home/hduser/IdeaProjects/SparkPractice1/src/main/resources/datasets/transactions.csv")

    val joinedDF = customerDF.join(transactionDF,"cc_num")
      .select("cc_num","first","last","trans_time","merchant","amt")


    val previousAmtWindow = Window.partitionBy("cc_num").orderBy("trans_time")

    val previousSwipeAmt = joinedDF.withColumn("Previous_Amt_Spent",
      lag(joinedDF("amt"),1).over(previousAmtWindow))

    previousSwipeAmt.show()
  }

}
