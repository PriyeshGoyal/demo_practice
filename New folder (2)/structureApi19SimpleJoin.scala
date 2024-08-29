import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.functions._

object structureApi19SimpleJoin extends App{
  
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  
  //creating spark conf
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  //creating spark session
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  
  val orderDf = spark.read
  .format("json")
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/order")
  .load()
  
  val customerDf = spark.read
  .format("json")
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week12/customer")
  .load()
  
  //customerDf.show()
  
  //val joinedDf = orderDf.join(customerDf, orderDf.col("order_customer_id") === customerDf.col("customer_id"),"inner")
  
  val joinCondition = orderDf.col("order_customer_id") === customerDf.col("customer_id")
  
  val joinType = "inner"
  
  val joinDf = orderDf.join(customerDf, joinCondition, joinType)

  joinDf.show()
  
  
  
}