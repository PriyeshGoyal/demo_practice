import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import java.sql.Timestamp


//import org.apache.spark.sql.types.IntegerType
//import org.apache.spark.sql.types.StringType
//import org.apache.spark.sql.types.TimestampType
//import org.apache.spark.sql.types.StructType
//import org.apache.spark.sql.types.StructField




case class ordersData (order_id : Int, order_data: Timestamp, order_customer_id: Int, order_status : String)

object DataFrameworkdemo extends App {
  
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
 /* val orderSchema = StructType(List(
  StructField("order_id", IntegerType)
  StructField("order_date", TimestampType)
  StructField("customer_id", IntegerType)
  StructField("order_status", StringType)
  ))
  
  */
  
  //shortcut way to load/read a file
  val orderDf : Dataset[Row] = spark.read
  .option("header",true)
  .option("inferSchema",true)
  .csv("C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/orders.csv")
  
  
  /*val orderDf = spark.read
  .format("csv")
  .option("header",true)
  .schema(orderSchema)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/orders.csv")
  .load
  */
  
  
  
  //orderDf.filter("order_ids < 10").show  //this shows runtime error bcz its a dataframe
  import spark.implicits._
  
  
  
  val ordersDs = orderDf.as[ordersData]
  
  ordersDs.filter(x=> x.order_id < 10)
  
  
  spark.close()
}