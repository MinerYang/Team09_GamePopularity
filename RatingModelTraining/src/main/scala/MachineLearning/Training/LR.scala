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
    // start training
    val Array(trainingSet, testSet) = featuredf.randomSplit(Array[Double](0.7, 0.3), 500)
    val lr_model = lr.fit(trainingSet)
    println("model training complete")

    //TODO
    val predictions = lr_model.transform(testSet)
    predictions.select("ratings","prediction", "probability").show(5)
    println("LR model accuracy:" + lr_model.summary.accuracy)



  }

}
