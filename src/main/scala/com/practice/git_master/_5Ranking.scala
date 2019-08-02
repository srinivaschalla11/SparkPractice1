package com.practice.git_master



import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{desc,rank,dense_rank}
import org.apache.spark.sql.types._

object _5Ranking {
 def main(args:Array[String]):Unit = {
   val spark = SparkSession.builder()
     .appName("Window_Ranking")
     .master("local")
     .getOrCreate()

   val myRows = Seq(Row(1,"srinivas",4000),
     Row(2,"randy",5000),
     Row(1,"sandy",9000),
     Row(1,"chuka",4000),
     Row(1,"adam",5000),
     Row(2,"gil",1500),
     Row(2,"rowdy",4000)
   )
   val rdd = spark.sparkContext.parallelize(myRows)

   val sch = StructType(StructField("class",IntegerType,false)::
     StructField("name",StringType,false)::
     StructField("salary",IntegerType,false)::Nil)
   val new_df = spark.createDataFrame(rdd,sch)

   new_df.show()

   val grouped_DF = Window.partitionBy("class").orderBy(desc("salary"))
   import spark.implicits._;
   val ranking = new_df.select('*,dense_rank().over(grouped_DF).as("rank"))


   ranking.filter('rank < 3).show()
 }
}
