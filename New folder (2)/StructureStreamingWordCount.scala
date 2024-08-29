import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

object StructureStreamingWordCount extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder()
  .master("local[2]")
  .appName("My Streaming app")
  .config("spark.sql.	shuffle.partition", 3)
  .config("spark.streaming.stopGracefullyOnShutdown", true)
  .getOrCreate()
  
  
// 1. read from the stream
  val linesDf = spark.readStream
  .format("socket")
  .option("host", "localhost")
  .option("port", "12345")
  .load()
  
  
  
  
 // 2. process
  val wordsDf = linesDf.selectExpr("explode(split(value,',')) as word ")
  
  val countsDf = wordsDf.groupBy("word").count()
  
  
  
 // 3. write to sink
  val wordCountQuery  = countsDf.writeStream
  .format("console")
  .outputMode("complete")
  .option("checkpointLocation", "checkpoint-location1")
  .trigger(Trigger.ProcessingTime("30 seconds"))
  .start()
  
  wordCountQuery.awaitTermination()
}