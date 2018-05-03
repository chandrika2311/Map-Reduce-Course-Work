package com.chandrika.HW4.HW4
import org.apache.spark.SparkConf
import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

object PageRankMain {
   def main(args: Array[String]) = {  
    val bz2Parser = new Bz2WikiParser()
    //Start the Spark context
    val conf = new SparkConf()
      .setAppName("PageRank")
      .setMaster("local")
    val sc = new SparkContext(conf)
    
    //Read some example file to a test RDD
    var filename = "/Users/chandrikasharma/Documents/Spring2018/MR/input/wikipedia-simple-html.bz2"
    
    var filename1="/Users/chandrikasharma/Documents/testgraph.txt"

    val listOfLines = sc.textFile(args(0))

    
   val result = listOfLines.map(line => (new Bz2WikiParser()).parsing(line))
//   BZ2 Parser sends Invalid as a string when the html file, name contains (~). 
//   The following line filters the data for that.
   val rdd2=result.filter(x=>x!="Invalid") 
//-----------------------------------------------------------------------------------   
//Each line is seperated by tab, and so the split rdd is RDD[Array[String]] 
//-----------------------------------------------------------------------------------   
    val splitRdd = rdd2.map( line => line.split("\t"))
    
//--------------------------------------------------------------------------------------------    
//newRdd does the basic processing of seperating both the strings seperated by tab and 
//    creating an RDD with Link and List of Outgoing links of structure RDD[(String, List[String])]
//---------------------------------------------------------------------------------------------------    
   val newRdd = splitRdd.map( arr => { var leng = arr.length
                        var neighboursStr=""
                         val link = arr( 0 ).trim
                         neighboursStr = arr( 1 )
//                         }
                         val neighbourList = neighboursStr.split( "," ).toList
                         val trimmedList = neighbourList.map(x=> x.trim)
                         (link,trimmedList)
                       }) 
//-------------------------------------------------------------------------------------------------------------------
//Handling dangling nodes here: 
// I take all the neighbouring nodes from each redord and emit an RDD[(String, List[String])] where values are List("dang")
// Reducing by key and removing all duplicate values finally resulting into a  RDD[(String, List[String])] of just Dangling nodes                      
//-------------------------------------------------------------------------------------------------------------------     
    val danglingRdd = newRdd.flatMapValues(x=>x).map(x=> (x._2.trim,List("dang"))).reduceByKey(_++_)
     val Distinctdang = danglingRdd.map(x=>(x._1.trim,x._2.distinct))
//Union the RDD with Dangling nodes and all other valid nodes  
     val finalRdd = newRdd.union(Distinctdang)
//Reducing by key to make handle all dangling nodes
    val reducedRdd = finalRdd.reduceByKey(_++_)
    val reducedDistinct = reducedRdd.map(x=> (x._1.trim,x._2.distinct))

//-------------------------------------------------------------------------------------------------------------------
//Graph size calculated by size of the Rdd    
//-------------------------------------------------------------------------------------------------------------------
    val graphSize = reducedDistinct.count
//setting initial Page ranks    
    val initialPR = 1.0/graphSize
//Each record is mapped with an initial page rank value
    var ranks = reducedDistinct.mapValues(v=>initialPR)
    val alpha = 0.15
    var contribution1 = reducedRdd.join(ranks)
//-------------------------------------------------------------------------------------------------------------------    
//    Getting the page ranks mass of dangling nodes:
//-------------------------------------------------------------------------------------------------------------------    
    for (i <- 1 to 11) { 
    val danglingPageRank = contribution1
                      .values
                      .filter(_._1.size<=1)
                      .filter(_._1.contains("dang"))
                      .map(_._2)
                      .sum()
       println("Dangling: ",danglingPageRank) 
//-------------------------------------------------------------------------------------------------------------------       
//Iteratively calculating the page rank by checking the size of the links
//-------------------------------------------------------------------------------------------------------------------       
     ranks = contribution1.values
                          .flatMap{ case(links, rank) =>    // RDD2 -> conbrib RDD
                               val size = links.size
                               links.map(url => (url, rank / size))
                         }.reduceByKey(_+_).mapValues(0.85*_ +0.15/graphSize +(0.85*danglingPageRank/graphSize))
       contribution1 =reducedDistinct.join(ranks)
    }
//-------------------------------------------------------------------------------------------------------------------    
//calculating top k    
//-------------------------------------------------------------------------------------------------------------------    
val top100 = ranks.sortBy(_._2, false).take(100)
val top100Rdd = sc.parallelize(top100)
top100Rdd.saveAsTextFile(args(1))
   sc.stop()
   }}   
    
    

