package com.practice.textbook

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{months_between,expr,lit,desc,col,current_date,to_date,current_timestamp,date_sub,date_add,datediff}

object _4DateOperations {

  def main(args:Array[String]):Unit = {

    val spark = SparkSession.builder()
      .appName("_4SparkSql")
      .master("local")
      .getOrCreate()

    val df = spark.read.format("csv").option("header","true")
      .option("inferSchema","true").load("/home/hduser/data_sets/family.csv")

    df.sort(desc("salary")).show();

    df.printSchema()
    df.createOrReplaceTempView("family")
    val new_df = spark.sql("select * from family where salary > 20000")
    new_df.show();

    df.where(col("salary").equalTo(35000)).select("id","name").show()
    df.where(col("salary") === 35000).select("id").show()

    val less_sal = col("salary") > 5000
    val more_sal = col("salary") < 40000
    df.where(less_sal.and(more_sal)).where(col("salary").isin(25000,30000,35000)).show()

    val date_df = df.withColumn("today",current_date())
    val time_df = date_df.withColumn("time",current_timestamp())

    val newtime_df = time_df.select(date_add(col("today"),5).alias("now"))
   // time_df.select(date_sub(col("today"),5)).show()
    // newtime_df.show()
    newtime_df.select(expr("*"),to_date(lit("1993-11-07")).alias("Bday")).show()
    newtime_df.select(datediff(col("now"),current_date())).show()
   // time_df.show();

    newtime_df.select( months_between(to_date(lit("2019-05-04")).alias("now"),
      to_date(lit("1993-11-07")).alias("bday"))
                     ).show()
    newtime_df.na.drop("all")
    newtime_df.na.drop("all",Seq("now","Bday"))

    newtime_df.na.fill(5,Seq("now","Bday"))
    val fillColValues = Map("now" -> 5, "Bday" -> 10)
    newtime_df.na.fill(fillColValues)


  }

}
