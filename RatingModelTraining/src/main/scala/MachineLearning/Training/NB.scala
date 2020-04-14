package MachineLearning.Training

import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.reflect.io.File

object NB {
  lazy val appName = "NB"
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

    evaluator1.setMetricName("f1")
    val f1 = evaluator1.evaluate(predictions)
    println("f1:" + f1)
    evaluator1.setMetricName("weightedPrecision")
    val prec = evaluator1.evaluate(predictions)
    println("precision:"  + prec)
    evaluator1.setMetricName("weightedRecall")
    val recall = evaluator1.evaluate(predictions)
    println("recall:" + recall)
    evaluator1.setMetricName("accuracy")
    val ac = evaluator1.evaluate(predictions)
    println("accuracy:" + ac)
    //    Test set accuracy = 0.7132038597776964
    //    f1:0.6481612995952053
    //    precision:0.6272876230988563
    //    recall:0.7132038597776963
    //    accuracy:0.7132038597776964

  }


}
