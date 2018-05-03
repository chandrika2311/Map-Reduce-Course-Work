package project
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.storage.StorageLevel
import org.apache.spark.mllib.tree.RandomForest
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object RandomForest_class {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Classification1")
    
    val sc = new SparkContext(conf)
    
    val inputPath: String = args(0) + "/training/"
    val testInputPath: String = args(0) + "/test/"
    val validationPath: String = args(0) + "/validation/"
    val modelOutputPath1: String = args(1) + "/model1/"
    val modelOutputPath2: String = args(1) + "/model2/"
    val modelOutputPath3: String = args(1) + "/model3/"
    val modelOutputPath4: String = args(1) + "/model4/"
    val modelOutputPath5: String = args(1) + "/model5/"
    val modelOutputPath6: String = args(1) + "/model6/"
    val modelOutputPath7: String = args(1) + "/model7/"
    val modelOutputPath8: String = args(1) + "/model8/"
    val resultOutputPath1: String = args(1) + "/result1/"
    val resultOutputPath2: String = args(1) + "/result2/"
    val resultOutputPath3: String = args(1) + "/result3/"
    val resultOutputPath4: String = args(1) + "/result4/"
    val resultOutputPath5: String = args(1) + "/result5/"
    val resultOutputPath6: String = args(1) + "/result6/"
    val resultOutputPath7: String = args(1) + "/result7/"
    val resultOutputPath8: String = args(1) + "/result8/"
    val predictionOutputPath: String = args(1) + "/prediction/"

    val rdd: RDD[String] = sc.textFile(inputPath)
  
    val validationRDD: RDD[String] = sc.textFile(validationPath)
    
    //For Train Data
    val converted: RDD[Array[Double]] = rdd.map(line =>line.split(",")).map(_.map(y => y.toDouble))
    var temp = converted.first//Just to create an action on the rdd
    //    For validation Data
    val convertedValidation:  RDD[Array[Double]] = validationRDD.mapPartitions(x=>x.map(line =>line.split(",")).map(_.map(y => y.toDouble)))
//    ---------------------------------------
    def mirror_y(arr: Array[Double]):Array[Double] ={
       val d = arr.grouped(441).toArray
       d.map(x=>x.grouped(21).toArray.map(d => d.reverse).flatMap(a=>a)).flatMap(s=>s)
       
    }
//    ---------------------------------------
    def rotate_90(arr: Array[Double]):Array[Double] ={
    
      val f = arr.grouped(441).toArray.map(x=>{
        var d = new ArrayBuffer[Double](441)
        for (i<-0 to 20){
            d.append(arr(i))
           for(j<-1 to 20){
             var f = i+(j*21)
             d.append(arr(f))
             }
            }
        d.toArray
        }
      )
      println(f.length)
      return f.flatMap(x=>x)
    }
//    ---------------------------------------
    def rotate_270(arr: Array[Double]):Array[Double] ={
      val d = rotate_90(arr)
      d.grouped(21).toArray.map(x=>x.reverse).flatMap(x=>x)
    }
//    ---------------------------------------
    def rotate_180(arr: Array[Double]):Array[Double] ={
      val d = arr.grouped(441).toArray
       d.map(x=>x.grouped(21).toArray.reverse.flatMap(s=>s)).flatMap(s=>s)
  }
//    ---------------------------------------
    val dense_mirror_y1: RDD[(Double,Array[Double])] = converted.mapPartitions(x=>x.map(line =>(line(3087),mirror_y(line.slice(0, 3087))))).persist(StorageLevel.DISK_ONLY)
    val dense_mirror_y: RDD[LabeledPoint] = dense_mirror_y1.mapPartitions(x=>x.map(line => LabeledPoint(line._1, Vectors.dense(line._2))))
    
    
    val dense_rotate_90: RDD[LabeledPoint] = dense_mirror_y1.mapPartitions(x=>x.map(line => LabeledPoint(line._1, Vectors.dense(rotate_90(line._2)))))  
    
  
    val dense_rotate_270: RDD[LabeledPoint] = dense_mirror_y1.mapPartitions(x=>x.map(line => LabeledPoint(line._1, Vectors.dense(rotate_270(line._2)))))
    
    
    val dense_90_m: RDD[LabeledPoint] = dense_mirror_y1.mapPartitions(x=>x.map(line => LabeledPoint(line._1, Vectors.dense(mirror_y(rotate_90(line._2))))))
    
    
    val dense_270_m: RDD[LabeledPoint] = dense_mirror_y1.mapPartitions(x=>x.map(line => LabeledPoint(line._1, Vectors.dense(mirror_y(rotate_270(line._2)))))) 
    
   
    dense_mirror_y1.unpersist()
    
    
    val dense_rotate_180: RDD[LabeledPoint] = converted.mapPartitions(x=>x.map(line => LabeledPoint(line(3087), Vectors.dense(rotate_180(line.slice(0, 3087))))))  
   
    
    val dense: RDD[LabeledPoint] = converted.mapPartitions(x=>x.map(line => LabeledPoint(line(3087), Vectors.dense(line.slice(0, 3087)))))
   
    
    val dense_180_m: RDD[LabeledPoint] = converted.mapPartitions(x=>x.map(line => LabeledPoint(line(3087), Vectors.dense(mirror_y(rotate_180(line.slice(0, 3087)))))))
   
   
    val denseValidation: RDD[LabeledPoint] = convertedValidation.mapPartitions(x=>x.map(line => LabeledPoint(line(3087), Vectors.dense(line.slice(0, 3087)))))

    val (trainingData1, testData1) = (dense, denseValidation)
    val (trainingData2, testData2) = (dense_mirror_y, denseValidation)
    val (trainingData3, testData3) = (dense_rotate_90, denseValidation)
    val (trainingData4, testData4) = (dense_rotate_180, denseValidation)
    val (trainingData5, testData5) = (dense_rotate_270, denseValidation)
    val (trainingData6, testData6) = (dense_90_m, denseValidation)
    val (trainingData7, testData7) = (dense_180_m, denseValidation)
    val (trainingData8, testData8) = (dense_270_m, denseValidation)

    // Train a RandomForest model.
    // Empty categoricalFeaturesInfo indicates all features are continuous.
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 30 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
    val maxDepth = 30
    val maxBins = 32
    
    val model1 = RandomForest.trainClassifier(trainingData1, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val model2 = RandomForest.trainClassifier(trainingData2, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val model3 = RandomForest.trainClassifier(trainingData3, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val model4 = RandomForest.trainClassifier(trainingData4, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val model5 = RandomForest.trainClassifier(trainingData5, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val model6 = RandomForest.trainClassifier(trainingData6, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val model7 = RandomForest.trainClassifier(trainingData7, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    val model8 = RandomForest.trainClassifier(trainingData8, numClasses, categoricalFeaturesInfo,numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
    
    // Evaluate model on test instances and compute test error

    val labelAndPreds1 = testData1.map { point =>
      val prediction = model1.predict(point.features)
      (point.label, prediction)
    }
    
    
    val labelAndPreds2 = testData2.map { point =>
      val prediction = model2.predict(point.features)
      (point.label, prediction)
    }
    val labelAndPreds3 = testData3.map { point =>
      val prediction = model3.predict(point.features)
      (point.label, prediction)
    }
    
    val labelAndPreds4 = testData4.persist().map { point =>
      val prediction = model4.predict(point.features)
      (point.label, prediction)
    }
    val labelAndPreds5 = testData5.persist().map { point =>
      val prediction = model5.predict(point.features)
      (point.label, prediction)
    }
    val labelAndPreds6 = testData6.persist().map { point =>
      val prediction = model6.predict(point.features)
      (point.label, prediction)
    }
    val labelAndPreds7 = testData7.persist().map { point =>
      val prediction = model7.predict(point.features)
      (point.label, prediction)
    }
    val labelAndPreds8 = testData8.persist().map { point =>
      val prediction = model8.predict(point.features)
      (point.label, prediction)
    }
    
    
    val testErr1 = labelAndPreds1.filter(r => r._1 != r._2).count.toDouble / testData1.count()
    val testErr2 = labelAndPreds2.filter(r => r._1 != r._2).count.toDouble / testData2.count()
    val testErr3 = labelAndPreds3.filter(r => r._1 != r._2).count.toDouble / testData3.count()
    val testErr4 = labelAndPreds4.filter(r => r._1 != r._2).count.toDouble / testData4.count()
    val testErr5 = labelAndPreds5.filter(r => r._1 != r._2).count.toDouble / testData5.count()
    val testErr6 = labelAndPreds6.filter(r => r._1 != r._2).count.toDouble / testData6.count()
    val testErr7 = labelAndPreds7.filter(r => r._1 != r._2).count.toDouble / testData7.count()
    val testErr8 = labelAndPreds8.filter(r => r._1 != r._2).count.toDouble / testData8.count()
    

    model1.save(sc, modelOutputPath1)
    model2.save(sc, modelOutputPath2)
    model3.save(sc, modelOutputPath3)
    model4.save(sc, modelOutputPath4)
    model5.save(sc, modelOutputPath5)
    model5.save(sc, modelOutputPath4)
    model6.save(sc, modelOutputPath5)
    

//-------------------------

    val array1: Array[(String, Long)] = Array(
      ("err", (testErr1 * 100000000).toLong)
      
    )
    val array2: Array[(String, Long)] = Array(
      ("err", (testErr2 * 100000000).toLong)
      
    )
    val array3: Array[(String, Long)] = Array(
      ("err", (testErr3 * 100000000).toLong)
      
    )
//    val array4: Array[(String, Long)] = Array(
//      ("size", size_4),
//      ("a1p1", a1p1_4),
//      ("a1p0", a1p0_4),
//      ("a0p0", a0p0_4),
//      ("a0p1", a0p1_4),
//      ("err", (testErr4 * 100000000).toLong)
//      
//    )
//    val array5: Array[(String, Long)] = Array(
//      ("size", size_5),
//      ("a1p1", a1p1_5),
//      ("a1p0", a1p0_5),
//      ("a0p0", a0p0_5),
//      ("a0p1", a0p1_5),
//      ("err", (testErr5 * 100000000).toLong)
//      
//    )

    /**
      * save accuracy
      */
    sc.parallelize(array1).saveAsTextFile(resultOutputPath1)
    sc.parallelize(array2).saveAsTextFile(resultOutputPath2)
    sc.parallelize(array3).saveAsTextFile(resultOutputPath3)
//    sc.parallelize(array4).saveAsTextFile(resultOutputPath4)
//    sc.parallelize(array5).saveAsTextFile(resultOutputPath5)


    sc.stop()

  }
}