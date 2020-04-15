package MachineLearning.Training

import org.apache.spark.ml.classification.MultilayerPerceptronClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.sql.{DataFrame, SparkSession}

object MLP {
  lazy val appName = "MLP"
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

    val layers = Array[Int](980, 64, 24, 5)
    val mlp = new MultilayerPerceptronClassifier()
      .setLayers(layers)
      .setBlockSize(128)
      .setMaxIter(100)

    //start trainig
    val Array(trainingSet, testSet) = featuredf.randomSplit(Array[Double](0.7, 0.3), 500)
    val mlp_model = mlp.fit(trainingSet)
    println("model training complete")

    //evaluation
    val predictions = mlp_model.transform(testSet)
    predictions.select("ratings","label","prediction", "probability").show(5)
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")
    println(s"Test Error = ${(1.0 - accuracy)}")

    evaluator.setMetricName("f1")
    val f1 = evaluator.evaluate(predictions)
    println("f1:" + f1)
    evaluator.setMetricName("weightedPrecision")
    val prec = evaluator.evaluate(predictions)
    println("precision:"  + prec)
    evaluator.setMetricName("weightedRecall")
    val recall = evaluator.evaluate(predictions)
    println("recall:" + recall)
    //    Test set accuracy = 0.7053418279833374
    //    Test Error = 0.29465817201666256
    //    f1:0.6086967399139426
    //    precision:0.552129604464222
    //    recall:0.7053418279833374

  }
}
