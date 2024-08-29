import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object TopMovies extends App {
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","topmovies")
   
  val ratingRdd = sc.textFile("/C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/ratings.dat")
  
  val mappedRdd = ratingRdd.map(x=> {
    
    val fields = x.split("::")
    (fields(1),fields(2))
  }) 
  
  //input 
  //(1193,5)
  //(1193,4)
  
  //output
  //(1193,(5,1))
  //(1193,(5.0,1.0))  after covert to float
  
  val newMappedRdd = mappedRdd.mapValues(x=> (x.toFloat,1.0))
  
  //input
  //(1193,(5.0,1.0))
  
  //output
  //(1193,(12.0,3.0)
  
  val reduceRdd = newMappedRdd.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
  
  //input
  //(1193,(12.0,3.0)
  
  val filteredRdd = reduceRdd.filter(x=> x._2._2 > 10)
  
   //input
  //(1193,(12000.0,3000.0)
  
  //output
  //(1193,4)
  
  val ratingsProcessed = filteredRdd.mapValues(x=> x._1/x._2).filter(x=> x._2 > 4.0)
  
  //ratingsProcessed.collect().foreach(println)
  
  val moviesRdd = sc.textFile("/C:/Users/Lenovo/Desktop/Sparkdepth/Spark_week11/movies.dat")
  
  val moviesMapped = moviesRdd.map(x => {
    
   val fields = x.split("::")
   
   (fields(0),fields(1))
  
  })
  
  
  val joinedRdd = moviesMapped.join(ratingsProcessed)
  
  val topMovies = joinedRdd.map(x=> x._2._1)
  
  topMovies.collect().foreach(println)
  
  scala.io.StdIn.readLine()
} 