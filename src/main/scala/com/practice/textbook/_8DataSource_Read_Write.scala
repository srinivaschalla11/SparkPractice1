package com.practice.textbook

import org.apache.spark.sql.SparkSession

object _8DataSource_Read_Write {

  def main(args:Array[String]):Unit = {
    val spark = SparkSession.builder()
      .appName("DataSource")
      .master("local")
      .getOrCreate()

    import spark.implicits._
    //WriteOperation

    val df = spark.sparkContext.parallelize(Seq(
      (1,"srinivas",2000),
      (2,"prasanna",3000),
      (3,"kumar",4000),
      (4,"challa",5000)
    )
    ).toDF("id","name","salary")



    df.write.mode("overwrite").option("sep","\t").save("hdfs://localhost:9000/user/df.tsv")
    df.write.mode("overwrite").format("json").save("hdfs://localhost:9000/user/df.json")
    df.write.mode("overwrite").format("parquet").save("hdfs://localhost:9000/user/df.parquet")
    df.write.mode("overwrite").format("orc").save("hdfs://localhost:9000/user/df.orc")

    df.write.mode("overwrite").format("csv").save("hdfs://localhost:9000/user/students.csv")
    df.write.mode("overwrite").format("parquet").save("hdfs://localhost:9000/user/student.parquet")


  }

}
