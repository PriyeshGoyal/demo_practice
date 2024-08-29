import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructuredStreamSess5 extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .master("local[2]")
  .appName("My Streaming app")
  .config("spark.sql.shuffle.partition", 3)
  .config("spark.streaming.stopGracefullyOnShutdown", true)
  .config("spark.sql.streaming.schemainference", true)
  .getOrCreate()
  
  
// 1. read from file source
  val ordersDf = spark.readStream
  .format("json")
  .option("path", "muinputFolder")
  .option("maxFileTrigger",1)
  .load()
  
  
  
  
 // 2. process
  ordersDf.createOrReplaceTempView("orders")
  
  val completeOrder = spark.sql("select * from orders where order_status = 'COMPLETE'")
  
  
  
 // 3. write to sink
  val orderQuery  = completeOrder.writeStream
  .format("console")
  .outputMode("complete")
  .option("checkpointLocation", "checkpoint-location1")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start()
  
  orderQuery.awaitTermination()
}