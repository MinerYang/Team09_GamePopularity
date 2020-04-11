package app
import org.apache.spark.ml.classification._
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.tuning.{CrossValidator, ParamGridBuilder}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.{_}

object ML {
  def lr(data:DataFrame) = {
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
    println("best ElasticNetParam: " + lrModel.getElasticNetParam)
    println("best RegParam: " + lrModel.getRegParam)
    lrModel
  }

  def ovr

  def rf

  def mlp

  def nb


}
