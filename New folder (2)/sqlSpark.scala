import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object sqlSpark extends App {
  
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name","SqlSpark")
  sparkConf.set("spark.master","local[2]")
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  val orderssDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path","C:/Users/Lenovo/Desktop/Trendy tech/Sparkdepth/Spark_week11/orders.csv")
  .load()
  
  orderssDf.createOrReplaceTempView("orders")  //table name orders which is distributed over the clusters
  
  
  //val resultDf = spark.sql("select order_status, count(*) as status_count from orders group by order_status order by status_count desc")
  
  val resultDff = spark.sql("select order_customer_id, count(*) as total_orders from orders where " +
      " order_status = 'CLOSED' group by order_customer_id order by total_orders desc")
      
  //resultDf.show()
  
  resultDff.show()
  
  scala.io.StdIn.readLine()
  
  //spark.close()
}