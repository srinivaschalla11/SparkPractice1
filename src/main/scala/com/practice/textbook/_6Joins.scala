package com.practice.textbook

import org.apache.spark.sql.SparkSession

object _6Joins {


   def main(args:Array[String]):Unit = {

     val spark = SparkSession.builder().
       appName("Joins")
       .master("local")
       .getOrCreate()

     import spark.implicits._

     val person = Seq((1,"srinivas",0),
       (2,"prasanna",1),
       (3,"kumar",1)).toDF("id","name ","graduate_program")

     val graduateProgram = Seq((0,"Masters","cse","CHS"),(2,"Masters","ECE","CHS"),(1,"PHD","ECE","CHS")).
       toDF("id","degree","department","school")

     val sparkStatus = Seq((500,"VP"),(250,"CEO"),(200,"MD")).toDF("id","status")

     val join_exp = person.col("graduate_program") === graduateProgram.col("id")
     val join_df = person.join(graduateProgram,join_exp)
     join_df.show()

     person.show()
     graduateProgram.show()
     sparkStatus.show()

    // inner-join

     person.join(graduateProgram,join_exp,"inner").show()
     person.join(graduateProgram,join_exp,"left_outer").show()
     person.join(graduateProgram,join_exp,"right_outer").show()
     person.join(graduateProgram,join_exp,"left_semi").show()
     person.join(graduateProgram,join_exp,"left_anti").show()
     person.join(graduateProgram,join_exp,"cross").show()



   }
}
