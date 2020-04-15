package ml

import org.apache.spark.ml.classification.{RandomForestClassificationModel, RandomForestClassifier}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession

case object RandomForestsML {
  lazy val appName = "SteamDataCleansing"
  lazy val master = "local[*]"

  def main(args: Array[String]) {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    // Load data
    val data = ss.read.format("libsvm")
      .load("hdfs://localhost:9000/CSYE7200/steam-data-for-ml")

    // Train a RandomForest model.
    val rf = new RandomForestClassifier()

    // Chain indexers and forest in a Pipeline.
    val pipeline = new Pipeline()
      .setStages(Array(rf))

    // Train model. This also runs the indexers.
    //    val model = pipeline.fit(trainingData)
    val paramMap = new ParamGridBuilder()
      .addGrid(rf.maxDepth, Array(10, 20, 30))
      .addGrid(rf.numTrees, Array(10, 30, 50))
      .build()

    val crossValidator = new CrossValidator()
      .setEstimator(pipeline)
      .setEstimatorParamMaps(paramMap)
      .setNumFolds(5)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setParallelism(2)

    val m = crossValidator.fit(data)
    val bm = m.bestModel.asInstanceOf[PipelineModel]
    val rfModel = bm.stages(0).asInstanceOf[RandomForestClassificationModel]
    println("best accuracy: " + m.avgMetrics.max + ", with num of trees: " + rfModel.getNumTrees + ", with max depth: " + rfModel.getMaxDepth)

    //////////////////////////////////////////////

    // Split the data into training and test sets (30% held out for testing).
    val Array(trainingData, testData) = data.randomSplit(Array(0.7, 0.3))

    // Make predictions.
    val predictions = rfModel.transform(testData)

    // Select example rows to display.
    predictions.select("prediction", "label", "features").show()

    // Select (prediction, true label) and compute test error.
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(predictions)
    println(s"Test Error = ${(1.0 - accuracy)}")

    //    val rfModel = model.stages(2).asInstanceOf[RandomForestClassificationModel]
    //    println(s"Learned classification forest model:\n ${rfModel.toDebugString}")
  }
}
