import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object streaminWordCountSlidingWin extends App {
  
  Logger.getLogger("org").setLevel(Level.ERROR)

  val sc =   new SparkContext("local[*]","wordcount")
  
  //creating spark streaming context
  val ssc = new StreamingContext(sc, Seconds(2))
  
  ssc.checkpoint(".")
  
  //lines is a Dstream
  val lines = ssc.socketTextStream("localhost", 9995)
  
  /*def updatefunc(newValues: Seq[Int], previousState: Option[Int]) : Option[Int] = {
    val newCount = previousState.getOrElse(0) + newValues.sum
    Some(newCount)
  } 
  */
  
  val wordCount = lines.flatMap(x => x.split(" ")).map(word => (word,1))
                  .reduceByKeyAndWindow((x,y) => x + y,((x,y)=> x-y),Seconds(10),Seconds(2))
                  .filter(x => x._2>0)
  
  
  wordCount.print()
  
  ssc.start()
  
  ssc.awaitTermination()
}
