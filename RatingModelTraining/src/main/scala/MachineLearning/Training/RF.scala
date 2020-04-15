package MachineLearning.Training

import org.apache.spark.ml.classification.{MultilayerPerceptronClassifier, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.{DataFrame, SparkSession}

object RF {
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

    featuredf.select("features").show()
    featuredf.printSchema()
//    val toDouble = udf[Double, String]( _.toDouble)
    def sparseToDense = udf((v: Vector) => v.toDense)
    val featuredf2 = featuredf.withColumn("features", sparseToDense(featuredf("features")))
    featuredf2.printSchema()

    val rf = new RandomForestClassifier()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setSeed(5043)

    //start trainig
    val Array(trainingSet, testSet) = featuredf2.randomSplit(Array[Double](0.7, 0.3), 5043)
    val rf_model = rf.fit(trainingSet)
    println("model training complete")

    //evaluation
    val predictions = rf_model.transform(testSet)
    predictions.select("ratings","label","prediction", "probability").show(5)
    predictions.printSchema()
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
    //    Test set accuracy = 0.6912111069790504
    //    Test Error = 0.3087888930209496
    //    f1:0.5650066895133307
    //    precision:0.4777727944112043
    //    recall:0.6912111069790504
  }

}
