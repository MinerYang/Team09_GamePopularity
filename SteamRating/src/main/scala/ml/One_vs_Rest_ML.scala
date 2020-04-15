package ml

import org.apache.spark.ml.classification.{LogisticRegression, OneVsRest}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

case object One_vs_Rest_ML {
  lazy val appName = "SteamDataCleansing"
  lazy val master = "local[*]"

  def main(args: Array[String]) {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    // Load data
    val inputData = ss.read.format("libsvm")
      .load("hdfs://localhost:9000/CSYE7200/steam-data-for-ml")

    // generate the train/test split.
    val Array(train, test) = inputData.randomSplit(Array(0.8, 0.2))

    // instantiate the base classifier
    val classifier = new LogisticRegression()
      .setMaxIter(20)
      .setRegParam(0.0001)
      .setElasticNetParam(0.5)
      .setTol(1E-6)
      .setFitIntercept(true)

    // instantiate the One Vs Rest Classifier.
    val ovr = new OneVsRest().setClassifier(classifier)

    // train the multiclass model.
    val ovrModel = ovr.fit(train)

    // score the model on test data.
    val predictions = ovrModel.transform(test)

    // obtain evaluator.
    val evaluator = new MulticlassClassificationEvaluator()
      .setMetricName("accuracy")

    // compute the classification error on test data.
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