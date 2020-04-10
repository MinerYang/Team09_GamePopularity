import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{IndexToString, StringIndexer, VectorAssembler}
import org.apache.spark.ml.mleap.SparkUtil
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.ml.classification.RandomForestClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.SparkSession

case object IrisSample{
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
    val inputColumns = Array("SepalLengthCm", "SepalWidthCm", "PetalLengthCm", "PetalWidthCm")
    val assembler = new VectorAssembler().setInputCols(inputColumns).setOutputCol("features")
    val featureDf = assembler.transform(rawData)
    //featureDf.show()
    /**
     * add label column
     */
    val indexer = new StringIndexer().setInputCol("Species").setOutputCol("indexedlabel")
    val lableIndexer = indexer.fit(featureDf)
    val labelDf = indexer.fit(featureDf).transform(featureDf)
//    for(i<- tmpindexer.labels)
//      println(i)

    /**
     * build a model using pipeline
     */
    val seed = 5043
    val Array(pltrainingSet, pltestSet) = rawData.randomSplit(Array[Double](0.7, 0.3), seed)
    val randomForestClassifier = new RandomForestClassifier()
      .setLabelCol("indexedlabel")
      .setFeaturesCol("features")
      .setSeed(seed)

    // Convert indexed labels back to original labels.
    val labelConverter = new IndexToString()
      .setInputCol("prediction")
      .setOutputCol("predictedLabel")
      .setLabels(lableIndexer.labels)

//    val stages = Array(assembler, indexer, randomForestClassifier)
    val stages = Array(assembler, indexer, randomForestClassifier,labelConverter)
    val pipeline = new Pipeline().setStages(stages)

    val pipelineModel = pipeline.fit(pltrainingSet)
    val pipelinePredictionDf = pipelineModel.transform(pltestSet)
    pipelinePredictionDf.show(5)

    /**
     * save model locally
     */
    pipelineModel.write.overwrite().save("/Users/mineryang/Desktop/PipelineIrisModel3")





    //    import ml.combust.bundle.BundleFile
//    import org.apache.spark.ml.bundle.SparkBundleContext
//    import ml.combust.bundle.serializer.SerializationFormat
//    import resource._
//    import ml.combust.mleap.spark.SparkSupport._
//    //val sbc = SparkBundleContext().withDataset(pipeline.transform(dataframe))
//    for(bundle <- managed(BundleFile("jar:file:/Users/mineryang/Desktop/myscalamodel.zip"))){
//      model.writeBundle.save(bundle)
//    }


    /**
     * build model
     */
    //    val seed = 5043
    //    val Array(trainingSet, testSet) = labelDf.randomSplit(Array[Double](0.7, 0.3), seed)
    //
    //    //train model
    //    val randomForestClassifier = new RandomForestClassifier().setSeed(seed)
    //    val model = randomForestClassifier.fit(trainingSet)
    //    //model.save("/Users/mineryang/Desktop/IrisModel")
    //
    //    // test the model against the test set       // test the model against the test set
    //    val predictions = model.transform(testSet)
    //    // evaluate the model
    //    val evaluator = new MulticlassClassificationEvaluator().setLabelCol("label").setPredictionCol("prediction").setMetricName("accuracy")
    //
    //    System.out.println("accuracy: " + evaluator.evaluate(predictions))

  }
}
