package com.practice.textbook

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.functions.{lit,asc,col,desc}

object _3SparkSql {



  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder().
      appName("practice").
      master("local").
      getOrCreate()

    val sch = StructType(StructField("id",IntegerType,false)
      ::StructField("name",StringType,false)
      ::StructField("salary",IntegerType,false)
      ::Nil)

    val df = spark.read.format("csv").
      option("header","true").
      schema(sch).
      load("/home/hduser/data_sets/family.csv")


   val mod_df =  df.withColumn("loyal",lit(1))
    mod_df.show()
   val drop_df =  mod_df.drop("loyal")
    drop_df.show()

   mod_df.where("id > 5").show()
   mod_df.filter("id < 5").show()

   mod_df.select("name").distinct().show()
   mod_df.sort(desc("salary")).show()
   mod_df.select(col("salary")).orderBy(asc("salary")).show()

   println(mod_df.rdd.getNumPartitions)

   mod_df.repartition(4)
   println(mod_df.rdd.getNumPartitions)


    println("*********************************************")
     mod_df.show()
    println("*********************************************")
    val condition = "id > 5"
    mod_df.select("id","name","salary").where(condition).sort(desc("id")).show()

  }
}
