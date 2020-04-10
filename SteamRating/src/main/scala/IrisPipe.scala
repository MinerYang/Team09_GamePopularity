import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable.ArrayBuffer

object IrisPipe {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[1]")
      .appName("SparkIris")
      .getOrCreate()
    val rawData = spark.read.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("/Users/mineryang/Downloads/iris/Iris.csv")


    /**
     *identify feature colunms
     */
    val assembler = featureasb()
    val featureDf = assembler.transform(rawData)

    /**
     * add label column
     */
    val indexer = labelindex("Species")
    val indexerModel = indexer.fit(featureDf)
    val colarr  = new ArrayBuffer[String]
    for(item<-indexerModel.labels)
      colarr += item
    val arr = colarr.toArray
//    val labelDf = indexer.fit(featureDf).transform(featureDf)

    //construct estimator
    val seed = 5043
    val rf = new RandomForestClassifier()
      .setLabelCol("indexedlabel")
      .setFeaturesCol("features")
      .setSeed(seed)

    //label convert
    val labelConverter = convertlabel(arr)

    // construct stages and pipeline
    val stages = Array(assembler, indexer, rf,labelConverter)
    val pipeline = new Pipeline().setStages(stages)

    //model training
    val Array(pltrainingSet, pltestSet) = rawData.randomSplit(Array[Double](0.7, 0.3), seed)
    val pipelineModel = pipeline.fit(pltrainingSet)
    val pipelinePredictionDf = pipelineModel.transform(pltestSet)
    pipelinePredictionDf.show(5)

    /**
     * save model locally
     */
    pipelineModel.write.overwrite().save("/Users/mineryang/Desktop/PipelineIrisModel4.0")

  }

  def featureasb():VectorAssembler ={
    val inputColumns = Array("SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm")
    new VectorAssembler()
      .setInputCols(inputColumns)
      .setOutputCol("features")
  }

  def labelindex(colname:String):StringIndexer = {
    new StringIndexer()
      .setInputCol(colname)
      .setOutputCol("indexedlabel")
  }

  def convertlabel(arr:Array[String]):IndexToString = {
    new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(arr)
  }

}
