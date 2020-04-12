package MachineLearning.Training

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

object MLP {
  lazy val appName = "MLP"
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

    val layers = Array[Int](424, 64, 24, 3)
    val mlp = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setMaxIter(100)

    //start trainig
    val Array(trainingSet, testSet) = featuredf.randomSplit(Array[Double](0.7, 0.3), 500)
    val mlp_model = mlp.fit(trainingSet)
    println("model training complete")

    //TODO
    val predictions = mlp_model.transform(testSet)
    predictions.show()
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("indexedLabel")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")
    println(s"Test Error = ${(1.0 - accuracy)}")
  }
}
