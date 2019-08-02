package com.practice.textbook

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg,sum,desc,col,asc}

object _7WindowFunctions {
    def main(args:Array[String]):Unit = {
      val spark = SparkSession.builder()
        .appName("Window")
        .master("local")
        .getOrCreate()

     import spark.implicits._
      val customerDF = spark.sparkContext.parallelize(Seq(
        ("srini","2016-05-03",45.0),
        ("srini","2016-05-04",55.00),
        ("challa","2016-05-01",25.00),
        ("challa","2016-05-04",29.00),
        ("challa","2016-05-06",30.00)
      )).toDF("name","date","amount_spent")

      val MovingAvgWindowSpec = Window.partitionBy("name").orderBy("date").rowsBetween(-1,1)
      val MovingAvg = customerDF.withColumn("moving_avg",avg(customerDF.col("amount_spent")).over(MovingAvgWindowSpec))

      val cumulativeSumWindow = Window.partitionBy("name").orderBy("date").rowsBetween(Long.MinValue,0)
      val cumulativeSum = customerDF.withColumn("cumulative_sum",sum(customerDF("amount_spent")).over(cumulativeSumWindow))

       val movingWindows = customerDF.withColumn("average",avg(customerDF.col("amount_spent"))
       .over(Window.partitionBy("name").orderBy(asc("name")).rowsBetween(-2,1)))

      movingWindows.show()
    //  MovingAvg.show()
    //  cumulativeSum.show()

    }
}
