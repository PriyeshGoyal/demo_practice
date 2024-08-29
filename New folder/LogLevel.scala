import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object LogLevel extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","loglevel")
  
  val myList = List("WARN: Tuesday 4 September 0405",
                    "ERROR: Tuesday 4 September 0408",
                    "ERROR: Tuesday 4 September 0408",
                    "ERROR: Tuesday 4 September 0408",
                    "ERROR: Tuesday 4 September 0408",
                     "ERROR: Tuesday 4 September 0408")

  val orignalLogsRdd = sc.parallelize(myList) //when we have a local collection then we use a 
                                              //parallelize to store it in RDD
  
  val newPairRdd = orignalLogsRdd.map( x => {
    
   val columns = x.split(":")
   val loglevel = columns(0)
   (loglevel,1)
  })
  
  val resultantRdd = newPairRdd.reduceByKey((x,y) => x + y )
  
  resultantRdd.collect().foreach(println)
  
  
  
}