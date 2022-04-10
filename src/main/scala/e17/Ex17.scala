package e17

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.explode

object Ex17 extends App {

  val spark = SparkSession
    .builder
    .appName("Spark SQL basic example")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._
  val nums = Seq(Seq(1,2,3)).toDF("nums")
  nums.withColumn("num", explode($"nums")).show
}
