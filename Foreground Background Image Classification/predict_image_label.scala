package project
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.tree.model.RandomForestModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.{SparkConf, SparkContext}


object predict_image_label {
  def main(args: Array[String]) {
    
    val conf = new SparkConf().setAppName("Classification1")
    
    val sc = new SparkContext(conf)
    
    val predictionOutputPath: String = args(1) + "/prediction/"
    val model1_path :String = args(1) + "/model1/"
    val model2_path :String = args(1) + "/model2/"
    val model3_path :String = args(1) + "/model3/"
    val model4_path :String = args(1) + "/model4/"
    val model5_path :String = args(1) + "/model5/"
    val model6_path :String = args(1) + "/model6/"
    val model7_path :String = args(1) + "/model7/"
    val model8_path :String = args(1) + "/model8/"
    val testInputPath: String = args(0) + "/test/"
    
     
    
    val model1 = RandomForestModel.load(sc,model1_path)
    val model2 = RandomForestModel.load(sc,model2_path)
    val model3 = RandomForestModel.load(sc,model3_path)
    val model4 = RandomForestModel.load(sc,model4_path)
    val model5 = RandomForestModel.load(sc,model5_path)
    val model6 = RandomForestModel.load(sc,model6_path)
    val model7 = RandomForestModel.load(sc,model7_path)
    val model8 = RandomForestModel.load(sc,model8_path)
    
// -------------------------Prediction
val testRDD: RDD[String] = sc.textFile(testInputPath)

    val convertedTestRDD: RDD[Array[Double]] = testRDD
      .map(line =>line.split(",")).map(line =>line.slice(0, 3087).map(x=>x.toDouble))
      
//      .map(_.map(y => {
//        if (y.equals("?")) {
//          0.0
//        } else {
//          y.toDouble
//        }
//      }))

    val denseTestRDD: RDD[LabeledPoint] = convertedTestRDD
      .map(line => LabeledPoint(0.0, Vectors.dense(line)))
    
    val predictionStart = System.currentTimeMillis()
    val predictedLabels: RDD[Int] = denseTestRDD.map { point =>
      var d = new ArrayBuffer[Int]()
      var r1 = model1.predict(point.features).toInt
      var r2 = model2.predict(point.features).toInt
      var r3 = model3.predict(point.features).toInt
      var r4 = model4.predict(point.features).toInt
      var r5 = model5.predict(point.features).toInt
      var r6 = model6.predict(point.features).toInt
      var r7 = model7.predict(point.features).toInt
      var r8 = model8.predict(point.features).toInt
      var result = r1+r2+r3+r4+r5+r6+r7+r8
      if(result>=4){
        1
        
      }else{
        
        0
      }
    }
//   Output
    predictedLabels
   .coalesce(numPartitions = 1)
   .saveAsTextFile(predictionOutputPath) 
    
}}