package com.practice.git_master

import org.apache.spark.sql.SparkSession

object _1Trans_Amount_Per_CreditCard {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("Transaction amount per credit card of customer")
      .master("local")
      .getOrCreate()

    val transaction = spark.read.format("csv").option("header","true").option("inferSchema","true").load("/home/hduser/IdeaProjects/SparkPractice1/src/main/resources/datasets/transactions.csv")
     transaction.show(4)
    transaction.select("cc_num","merchant","amt").groupBy("cc_num","merchant").avg("amt").show(false)

  }

}
