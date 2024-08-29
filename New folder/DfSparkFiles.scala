import org.apache.spark.SparkConf
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession

object DfSparkFiles extends App{
  
  
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "my first Application")
  sparkConf.set("spark.master", "local[2]")
  
  
  val spark = SparkSession.builder()
  .config(sparkConf)
  .getOrCreate()
  
  

  
  //standard way to read.load a file
  /*val orderDf = spark.read
  .format("csv")
  .option("header",true)
  .option("inferSchema",true)
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/orders.csv")
  .load()
  */
  
  
  //loading json file
  /* val orderDf = spark.read
  .format("json")
  .option("mode","DROPMALFORMED")
  .option("path","C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/players.json")
  .load()
  */
  
  //loading parquet file 
  val orderDf = spark.read
  .option("path","C:/Users/Lenovo/Desktop/Trendy tech/Sparkdepth/Spark_week11/users.parquet")
  .load()
  
  
  orderDf.printSchema()
  orderDf.show()   //truncating the data 
  
  //spark.close()
  scala.io.StdIn.readLine()
}