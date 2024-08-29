import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object streamingWordCount extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc =   new SparkContext("local[*]","wordcount")
  
  //creating spark streaming context
  val ssc = new StreamingContext(sc, Seconds(5))
  
  //lines is a Dstream
  val lines = ssc.socketTextStream("localhost", 9995)
  
  def updatefunc(newValues: Seq[Int], previousState: Option[Int]) : Option[Int] = {
    val newCount = previousState.getOrElse(0) + newValues.sum
    Some(newCount)
  } 
  
  val words = lines.flatMap(x => x.split(" "))
  
  
  val pairs = words.map(x => (x,1))
  
  //val wordCount = pairs.reduceByKey((x,y)=> x+y)
  
  val wordCount = pairs.updateStateByKey(updatefunc)
  wordCount.print()
  
  ssc.start()
  
  ssc.awaitTermination()
}