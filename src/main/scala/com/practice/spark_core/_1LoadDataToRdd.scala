package com.practice.spark_core

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{explode, udf}

import scala.collection.mutable

case class CustomInput(a:Array[Integer],b:Array[String])
case class Result(id:Integer,name:String)

object _1LoadDataToRdd {

  def assertSameSize(arrs:Seq[_]*) = {
    assert(arrs.map(_.size).distinct.size==1,"sizes differ")
  }
  def main(args:Array[String]) = {
    val spark = SparkSession.builder()
      .appName("core-1")
      .master("local")
      .getOrCreate()

  /*  val transaction_Rdd = spark.sparkContext.textFile("/IdeaProjects/SparkPractice/src/main/resources/datasets/transaction.csv")

    var result = transaction_Rdd.collect().toList
    result.take(10).foreach(println)


    val schema = StructType(StructField("id",IntegerType,true)::
                            StructField("Name",ArrayType(StringType,true),true)::Nil)
    import spark.implicits._
    val data = spark.sparkContext.parallelize( (List(Array(1,2,3,4,5), Array("a","b","c","d"))))
    val df = spark.createDataFrame(data,schema)

   df.show()*/

    import spark.implicits._
    val df=Seq((Array(1,2,3),Array("a","b","c"))).toDF("ID","NAME")

    //val exe = df.withColumn("var",explode(custom($"ID",$"NAME"))).select($"var._1".alias("id"),$"var._2".alias("name"))
  //  exe.show()
 // val zip = udf((a:Array[Integer],b:Array[String])=> a.zip(b))

    df.createOrReplaceTempView("df1")
    spark.sql("select ID, NAME from df1 lateral view explode(ID) as id lateral view explode(NAME) as name").show()


   // val res = df.rdd.map(row => getResult(row))

  /*  df.rdd.map(row => {
      val id = row.getAs("ID")
      val name = row.getAs("name")
      CustomInput(id,name)
    })*/


    //val zip = udf((xa:Array[Integer],xb:Array[String]) => xa.zip(xb))
  //  df.withColumn("vars",explode(zip($"ID",$"NAME"))).select($"vars._1".alias("id"),$"vars._2".alias("name")).show()


  }


    // def get_res(id_array:Array[Integer],name_array:Array[String]):Iterator[Input]={

}
