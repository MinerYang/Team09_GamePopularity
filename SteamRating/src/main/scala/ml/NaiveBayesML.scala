package ml

import org.apache.spark.ml.classification.{NaiveBayes, NaiveBayesModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

case object NaiveBayesML {
  lazy val appName = "SteamDataCleansing"
  lazy val master = "local[*]"

  def main(args: Array[String]) {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    // Load data
    val data = ss.read.format("libsvm")
      .load("hdfs://localhost:9000/CSYE7200/steam-data-for-ml")

    val nb = new NaiveBayes()

    val pipeline = new Pipeline().setStages(Array(nb))
    val paramMap = new ParamGridBuilder()
      .addGrid(nb.smoothing, Array(0.0001, 0.001, 0.01, 0.1, 1))
      .build()

    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramMap)
      .setNumFolds(5)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setParallelism(2)

    val m = crossValidator.fit(data)
    val bm = m.bestModel.asInstanceOf[PipelineModel]
    val nbModel = bm.stages(0).asInstanceOf[NaiveBayesModel]
    println("best accuracy: " + m.avgMetrics.max + ", with smoothing para: " + nbModel.getSmoothing)

    /////////////////////////////////////////////////////////////////

    // Split the data into training and test sets (30% held out for testing)
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3), seed = 2378L)

    //    // Train a NaiveBayes model.
    //        val model = new NaiveBayes()
    //          .fit(trainingData)

    // Select example rows to display.
    val predictions = nbModel.transform(testData)
    predictions.show()

    //     Select (prediction, true label) and compute test error
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")

    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")
    val f1 = evaluator2.evaluate(predictions)
    println(s"Test set f1 = $f1")

    val evaluator3 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
    val weightedPrecision = evaluator3.evaluate(predictions)
    println(s"Test set weightedPrecision = $weightedPrecision")

    val evaluator4 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    val weightedRecall = evaluator4.evaluate(predictions)
    println(s"Test set weightedRecall = $weightedRecall")
  }
}


