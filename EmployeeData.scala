
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

case class Employee(name:String, gender:String, age:Int, dept:String, score:Int)

object EmployeeData extends App{
    val conf    = new SparkConf().setAppName("EmployeeData").setMaster("local")
    val sc      = new SparkContext(conf)
    val sqlCtxt = new SQLContext(sc)


    val empRDD = sc.textFile("employee.txt")
      .map(_.split(","))
      .map(e => Employee(e(0),e(1),e(2).trim.toInt,e(3),e(4).trim.toInt))

    import sqlCtxt.implicits._
    val empDF = empRDD.toDF

    //minimum, maximum and average age of the employee of each department
    val employee = empDF.groupBy("dept").agg(min("age"),max("age"),avg("age"),count("*") as "EmployeeCount")
    employee.show

    //show the avg age of employee based on the gender
    empDF.select("*").filter(empDF("gender") === "f").groupBy("dept").agg(avg("age")).show

    //rating based on the score of the employee
    empDF.withColumn("Rating",udfScoreToRatings.udfScoreToRating(empDF("score"))).show
}

