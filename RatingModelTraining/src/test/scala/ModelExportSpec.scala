import MachineLearning.PipelineTransfomer.{fs, indexer, t0, t1, t10, t11, t12, t2, t3, t4, t5, t6, t7, t8, t9}
import app.{DataCleaning, ModelExport}
import app.ModelExport.{appName, evaluation, master, readcsv}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class ModelExportSpec extends FlatSpec with Matchers{
  lazy val appName = "stagesConstruct"
  lazy val master = "local[*]"
  lazy val threshold = 0.05
  val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
  ss.sparkContext.setLogLevel("WARN")
  val path = "."
  val origindf = ss.read.parquet(s"$path/cleandata.parquet")
  val rawdf = readcsv(ss)
  val nb = new NaiveBayes()
  val stages = Array(indexer,t0,t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, fs, nb)
  val pipeline = new Pipeline()
    .setStages(stages)
  val Array(trainingSet, testSet) = origindf.randomSplit(Array[Double](0.7, 0.3), 500)
  val pipelineModel = pipeline.fit(trainingSet)


  behavior of "saveModel"
  it should "save model in /test/resources" in {

    ModelExport.saveModel(pipelineModel,"./src/test/resources")
    val model = PipelineModel.read.load("./src/test/resources/best_model") should matchPattern {
      case pipelineModel : PipelineModel =>
    }
  }

  behavior of "evaluation"
  it should "it should return a double val between 0 ~ 1" in {
    val accuracy = evaluation(pipelineModel,testSet)
    assert(accuracy <= 1 && accuracy >= 0)
  }

  behavior of "savePipeline"
  it should "save pipeline in /test/resources" in {
    val orin = ModelExport.initNBpipeline()
    ModelExport.savePipeline("./src/test/resources")
    val pip = Pipeline.read.load("./src/test/resources/my_pipeline") should matchPattern {
      case orin : Pipeline =>
    }
  }

  behavior of "saveToCsv"
  it should "should save dataframe to csv" in {
    ModelExport.saveToCsv(rawdf)
    val df = ModelExport.readcsv(ss)
    assert( rawdf.schema.equals(df.schema))
    assert( rawdf.collect().sameElements(df.collect()))
  }
}
