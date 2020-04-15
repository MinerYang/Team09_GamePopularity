package MachineLearning.Training

import org.apache.spark.sql.{DataFrame, SparkSession}
import MachineLearning.PipelineTransfomer._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{StringIndexer, VectorAssembler}
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}

object LR {
  lazy val appName = "LogsiticRegression"
  lazy val master = "local[*]"
  lazy val threshold = 0.05
  val path = "."

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    // import cleaned and preprocessed dataset
    val featuredf: DataFrame = ss.read.parquet(s"$path/featuredData.parquet")
    featuredf.show(5)
    featuredf.printSchema()
    println("featuredData load success")

    val lr = new LogisticRegression()
      .setMaxIter(20)
    val pipeline = new Pipeline()
      .setStages(Array(lr))
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01, 0.001, 0.0001))
      .addGrid(lr.elasticNetParam, Array(0.2, 0.4, 0.5, 0.7, 0.8))
      .build()
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5) // Use 3+ in practice
      .setParallelism(2) // Evaluate up to 2 parameter settings in parallel
    println("40 % completed")
    val Array(trainingSet, testSet) = featuredf.randomSplit(Array[Double](0.7, 0.3), 500)
    val cvModel = cv.fit(trainingSet)
    val lr_model: LogisticRegressionModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[LogisticRegressionModel]
    println("[lrModel] best accuracy: " + cvModel.avgMetrics.max)
    println("[lrModel] best ElasticNetParam: " + lr_model.getElasticNetParam + ", best RegParam: " + lr_model.getRegParam)

//    val lr_model = lr.fit(trainingSet)
    println("model training complete")

    //evaluation
    val predictions = lr_model.transform(testSet)
    predictions.select("ratings","label","prediction", "probability").show(5)
    println("LR model accuracy:" + lr_model.summary.accuracy)
    //    val arr = lr_model.summary.precisionByLabel
    //    var sum = 0.0
    //    for ( i <- 0 to (arr.length - 1)) {
    //      sum += arr(i);
    //    }
    //    println(sum + "   !!!  " + sum / 5 )
    val precision = lr_model.summary.weightedPrecision
    val recall = lr_model.summary.weightedRecall
    val f1 = lr_model.summary.weightedFMeasure
    println("LR model precision:" + precision)
    println("LR model recall:" + recall)
    println("LR model f1_score:" + f1)
    //LR model accuracy:0.758606102268521
    //LR model precision:0.727881489531847
    //LR model recall:0.758606102268521
    //LR model f1_score:0.7026606506894217


  }

}
