

object basics extends App {


  
//Write a Scala program to compute the sum of the two given integer values. If the two values are the same, then return triples their sum.  
  
/*def test(x: Int , y: Int): Int = { 
  if (x == y) (x + y) * 3 
  else 
    x + y
  }
println("if number is same :" + test(1,2))
println("if number is diff :" + test(3,3))
}




 
//Write a Scala program to check two given integers, and return true if one of them is 30 or if their sum is 30.  
  
  def test1(x: Int, y: Int): Boolean = {
    x == 30 || y == 30 || x + y == 30
    }
println("Result: " + test1(30, 0))
println("Result: " + test1(25, 5))
println("Result: " + test1(30, 20))
println("Result: " + test1(25, 20))
} 


  
//filter Even odd from the given list 
  
   //val nums = List(1, 2, 3, 4, 5, 7, 9, 11, 14, 12, 16)
   val nums = List.range(1,20)
   println("Original list:")
   println(nums)
   println()
   
   val even_nums = nums.filter(_ % 2 == 0) 
   println("Even number from the list:")
   println(even_nums)
   println()
   
   val odd_nums = nums.filter(_ % 2 != 0) 
   println("Odd number from the list:")
   println(odd_nums)   
} 
  
  
 // join the give list or merge the given the list
  
   val x = List.range(1,10)
   val y = List.range(10,20)
   
   println("Given List :")
   println(x)
   println(y)
   println()
   
   
   println("Merge the List with ++ :")
   val plus = x ++ y
   println(plus)
   println()
   
   println("Merge the Lsit with ::: ")
   val type2 = x ::: y
   println(type2)
   println()
   
   println("Merge the list with concat mehtod")
   val con = List.concat(x,y)
   println(con)
   
   val a = 10
   val b = 20
   
   val num = a + b
   println(num)
   
}   
   

  // adding to numbers 
   def numss(x : Int , y: Int) : Int = {
    x + y 
   }
println("Sum :" + numss(1,2))
}  





  
//Write a Scala program to get the absolute difference between n and 51. If n is greater than 51 return triple the absolute difference  
  
  
   def test(x : Int) : Int = {
     val abs_Diff = Math.abs(x - 51)
     if (x > 51)
       3 * abs_Diff
     else
       abs_Diff
       
   }
   println("Result :" +test(60))
   println("Result :" + test(80))
}  

 
// print fizzbuzz with scala
  
  def fizzbuzz( x: Int) = {
    if (x % 15 == 0)
      "fizzbuzz"
    else if (x % 3 == 0 )
      "fizz"
    else if (x % 5 == 0)
      "buzz"
    else 
      x  
  }
  println("result :" +fizzbuzz(3))
  
 
 
 
 
 
  * /// print fibonacii series 
  
  //object fibonaci extends App{

def fibonaci(n:Int)=
{
def fibgo(n:Int,prev:Int=0,next:Int=1):Int= n match
{
case 0 => prev
case 1 => next
case _ => fibgo(n-1,next,(next+prev))

}

fibgo(n)
}

println(fibonaci(8))
}
//}  


def addInt( a:Int, b:Int ) : Int = {
     var sum:Int = 0
      sum = a + b

      return sum
}
println("Returned Sum " + addInt(5,10))

 
  
  
 
 
 
 
 
 
  /// print factorial using loop 
def Fact(input: Int ) : Int = {
  var result : Int = 1
    for(i <- 1 to input)
  {
    result = result * i
  }
    result
}
println("factorial of :"  +Fact(4)) 

////
 * 
 * 
 * 
 * 
 * 
 * 
 *  
 /// print factorial of nth  number using recursion
  
 def factR(input : Int ) : Int = {
   if (input == 1) 
     1
   else input * factR(input - 1)
 }
println("result of factorial : " +factR(4))

*/

def factRT(input: Int, result : Int) : Int = {
  if (input == 1) 
    result
  else factRT(input-1, result * input)
}
println(+factRT(4,1))
}

