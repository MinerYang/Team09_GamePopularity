package ml

import org.apache.spark.ml.classification.{MultilayerPerceptronClassificationModel, MultilayerPerceptronClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

case object MultilayerPerceptroML {
  lazy val appName = "SteamDataCleansing"
  lazy val master = "local[*]"

  def main(args: Array[String]) {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    // Load data
    val data = ss.read.format("libsvm")
      .load("hdfs://localhost:9000/CSYE7200/steam-data-for-ml")



    // specify layers for the neural network:
    // input layer of size 4 (features), two intermediate of size 5 and 4
    // and output of size 3 (classes)
    //    val layers = Array[Int](424, 64, 24, 3)

    // create the trainer and set its parameters
    val mlp = new MultilayerPerceptronClassifier()
      .setBlockSize(128)
      .setMaxIter(100)

    val pipeline = new Pipeline().setStages(Array(mlp))
    val paramMap = new ParamGridBuilder()
      .addGrid(mlp.layers, Array(Array(980, 10, 5), Array(980, 34, 5), Array(980, 68, 5)))
      .build()

    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramMap)
      .setNumFolds(5)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setParallelism(2)

    val m = crossValidator.fit(data)
    val bm = m.bestModel.asInstanceOf[PipelineModel]
    val mlpModel = bm.stages(0).asInstanceOf[MultilayerPerceptronClassificationModel]
    println("best accuracy: " + m.avgMetrics.max + ", with hidden layer setting: " + mlpModel.layers(1))

    /////////////////////////////////////////////////////////////////
    // train the model
    //    val model = trainer.fit(train)

    // Split the data into train and test
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 5678L)
    val train = splits(0)
    val test = splits(1)

    // compute accuracy on the test set
    val result = mlpModel.transform(test)
    val predictionAndLabels = result.select("prediction", "label")
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictionAndLabels)
    println(s"Test set accuracy = $accuracy")

    val evaluator2 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("f1")
    val f1 = evaluator2.evaluate(predictionAndLabels)
    println(s"Test set f1 = $f1")

    val evaluator3 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedPrecision")
    val weightedPrecision = evaluator3.evaluate(predictionAndLabels)
    println(s"Test set weightedPrecision = $weightedPrecision")

    val evaluator4 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("weightedRecall")
    val weightedRecall = evaluator4.evaluate(predictionAndLabels)
    println(s"Test set weightedRecall = $weightedRecall")
  }
}