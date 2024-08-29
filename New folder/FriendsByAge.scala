import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object FriendsByAge extends App {
  
def parseLine(line: String) = {
  val fields = line.split("::")
  val age = fields(2).toInt
  val numFriends = fields(3).toInt
    (age,numFriends)
    }
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","ratingcalculator")
  
  val input = sc.textFile("/C:/Users/Lenovo/Downloads/friendsdata.csv")
  
  val mapInput = input.map(parseLine)
  
  //INPUT
  //(33,100)
  //(33,200)
  //(33,500)
  
  //OUTPUT
  //(33,(100,1))
  //(33,(200,1))
  //(33,(300,1))
  
  val mappedFinal = mapInput.map(x => (x._1,(x._2,1)))
  
  val totalAgeBy = mappedFinal.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))
  
  val avgAge = totalAgeBy.map(x => (x._1,x._2._1/x._2._2)).sortBy(x=> x._2)
  
  val results = avgAge.collect
  
  results.foreach(println)
}