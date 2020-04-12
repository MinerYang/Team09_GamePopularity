package MachineLearning.Training

import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

object NB {
  lazy val appName = "NB"
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


    val nb = new NaiveBayes()
    val Array(trainingSet, testSet) = featuredf.randomSplit(Array[Double](0.7, 0.3), 7777L)
    val nb_model = nb.fit(trainingSet)
    nb_model.transform(trainingSet).show(5)
    println("model training complete")

    //TODO
    val predictions = nb_model.transform(testSet)
    predictions.select("ratings","label","prediction", "probability").show(5)
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("ratings")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")
  }

}
