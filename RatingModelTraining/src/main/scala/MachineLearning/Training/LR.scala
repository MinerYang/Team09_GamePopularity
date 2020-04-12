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
  val path = "/Users/mineryang/Desktop/Team09_GamePopularity/RatingModelTraining"

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

    //TODO
    val predictions = lr_model.transform(testSet)
    predictions.select("ratings","label","prediction", "probability").show(5)
    println("LR model accuracy:" + lr_model.summary.accuracy)



  }

}
