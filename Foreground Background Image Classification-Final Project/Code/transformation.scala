package project
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.mutable.ListBuffer

object transformation {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Classification1").setAppName("Transformation")
      .setMaster("local")
    
    val sc = new SparkContext(conf)
    val m = sc.textFile("/Users/chandrikasharma/Downloads/test")
    
    val matrix = m.map( z=> {
      z.split(",")
    }).collect().flatten
    val r = matrix.grouped(441).toArray
   
    def mir_y(arr: Array[String]):Array[String] ={
       arr.grouped(21).toArray.map(d => d.reverse).flatten
    } 
    
    val mirror_o = r.map(x=> mir_y(x))
    
//    val mirror = mir_y(matrix)
//    println("mirrored result")
//    mirror_o.foreach(x=> x.foreach(println))
    
//   Function begin 
    
    def denseRotation_90(arr: Array[String]):Array[String] ={
      
      arr.grouped(21).toArray.map(d => d.reverse).flatten
      
      var d = new ListBuffer[String]()
      for (i<-0 to 20){
        d.append(arr(i))
       for(j<-1 to 20){
         var f = i+(j*21)
         d.append(arr(f))
      }}
      val z = d.toArray
       return z
       
    }
    
    
    val d_90 = r.map(x=> denseRotation_90(x))
    val d_270 = d_90.map(x=>x.grouped(21).toArray.map(d => d.reverse).flatten)
//    println("Rotation to 90 degree") 
//    d_90.foreach(x=> x.foreach(println))
    
    println("Rotation to 270 degree")
    d_270.foreach(x=> x.foreach(print))
//  ---------------------------------  
  println("Rotation to 180 degree")
  def rotate_180(arr: Array[String]):Array[String] ={
    arr.grouped(21).toArray.reverse.flatten
  }
  
  val d_180 = r.map(x=> rotate_180(x))
  d_180.foreach(x=> x.foreach(println))
  
  
  
//  ---------------------------------  
//  println("Rotation to 270 degree")
//  val x_r = matrix.reverse
//  val d_270 = denseRotation_90(x_r)
//  d_270.foreach(ar => ar.foreach(println))
  }
}