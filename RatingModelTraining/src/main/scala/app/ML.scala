package app

import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql._

object ML {
  def lr(data: DataFrame): LogisticRegressionModel = {
    val lr = new LogisticRegression()
      .setMaxIter(20)
    val pipeline = new Pipeline()
      .setStages(Array(lr))

    // We use a ParamGridBuilder to construct a grid of parameters to search over.
    // With 3 values for hashingTF.numFeatures and 2 values for lr.regParam,
    // this grid will have 3 x 2 = 6 parameter settings for CrossValidator to choose from.
    val paramGrid = new ParamGridBuilder()
      .addGrid(lr.regParam, Array(0.1, 0.01, 0.001, 0.0001))
      .addGrid(lr.elasticNetParam, Array(0.2, 0.4, 0.5, 0.7, 0.8))
      .build()

    // We now treat the Pipeline as an Estimator, wrapping it in a CrossValidator instance.
    // This will allow us to jointly choose parameters for all Pipeline stages.
    // A CrossValidator requires an Estimator, a set of Estimator ParamMaps, and an Evaluator.
    // Note that the evaluator here is a BinaryClassificationEvaluator and its default metric
    // is areaUnderROC.
    val cv = new CrossValidator()
      .setEstimator(pipeline)
      .setEvaluator(new MulticlassClassificationEvaluator)
      .setEstimatorParamMaps(paramGrid)
      .setNumFolds(5) // Use 3+ in practice
      .setParallelism(2) // Evaluate up to 2 parameter settings in parallel

    // Run cross-validation, and choose the best set of parameters.
    val cvModel = cv.fit(data)
    val lrModel: LogisticRegressionModel = cvModel.bestModel.asInstanceOf[PipelineModel].stages(0).asInstanceOf[LogisticRegressionModel]
    println("[lrModel] best ElasticNetParam: " + lrModel.getElasticNetParam)
    println("[lrModel] best RegParam: " + lrModel.getRegParam)
    lrModel
  }

  def rf(data: DataFrame): RandomForestClassificationModel = {
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
    println("[rfModel] best accuracy: " + m.avgMetrics.max + ", with num of trees: " + rfModel.getNumTrees + ", with max depth: " + rfModel.getMaxDepth)
    rfModel
  }

  def mlp(data: DataFrame): MultilayerPerceptronClassificationModel = {
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
    println("[mlpModel] best accuracy: " + m.avgMetrics.max + ", with hidden layer setting: " + mlpModel.layers(1))
    mlpModel
  }

  def nb(data: DataFrame): NaiveBayesModel = {
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
    println("[nbModel] best accuracy: " + m.avgMetrics.max + ", with smoothing para: " + nbModel.getSmoothing)
    nbModel
  }
}
