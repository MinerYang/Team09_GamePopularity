package app

import app.ML._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.{DataFrame, SparkSession}
import MachineLearning.PipelineTransfomer._
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes}


object ModelExport {
  lazy val appName = "stagesConstruct"
  lazy val master = "local[*]"
  lazy val threshold = 0.05

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    import ss.implicits._
    val path = "/Users/mineryang/Desktop/Team09_GamePopularity/RatingModelTraining"
    val origindf: DataFrame = ss.read.parquet(s"$path/cleandata.parquet")
    print("Completed : 30 %\n")
    /**
     * save selected featueres for futher use
     * printout featuring process for each transfomers
     * nothing to do with pipeline training
     */
//    saveFeatureProcess(origindf,path)
//    print("Completed : 50 %\n")
    /**
     * choose one of the best training model to export
     */
      val name="NB"
      val nb = new NaiveBayes()
      // construct new pipeline for web use
      val stages = Array(indexer,t0,t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, fs, nb)
      val pipeline = new Pipeline()
        .setStages(stages)
     print("Completed : 70 %\n")
      // start training
      val Array(trainingSet, testSet) = origindf.randomSplit(Array[Double](0.7, 0.3), 500)
      val pipelineModel = pipeline.fit(trainingSet)
      println("model training complete")

    val predictions = pipelineModel.transform(testSet)
    predictions.select("ratings","label","prediction", "probability").show(5)
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")

    /**
     * save model locally
     */
      val exportpath = s"$path/best_model"
      pipelineModel.write.overwrite().save(exportpath)
      println("This model has saved for futher use")


  }

  def saveFeatureProcess(origindf:DataFrame,path:String): Unit ={
    origindf.show(5)
    origindf.printSchema()

    /* start build pipeline */
    //label indexer
    val indexdf :DataFrame = indexer.fit(origindf).transform(origindf)
    //for price
    println("DF0 & DF2")
    val df0:DataFrame = t0.transform(indexdf)
    df0.printSchema()
    val df1: DataFrame = t1.fit(df0).transform(df0)
    val df2: DataFrame = t2.fit(df1).transform(df1)
    df2.printSchema()

    //for Platforms, Categories, Tags, Developer, Publisher
    println("DF3 & DF4 selection for platforms")
    val df4 = frameTransform(t3,t4,"platforms",df2,path)

    println("DF5 & DF6 selection for categories")
    val df6 = frameTransform(t5,t6,"categories",df4,path)

    println("DF7 & DF8 selection for tags")
    val df8 = frameTransform(t7,t8,"tags",df6,path)

    println("DF9 & DF10 selection for developer")
    val df10 = frameTransform(t9,t10,"developer",df8,path)

    println("DF9 & DF10 selection for publisher")
    val df12 = frameTransform(t11, t12,"publisher",df10,path)

    // for features
    println("features Assembler & fs_DF")
    val fsdf: DataFrame = fs.transform(df12)
    fsdf.show(5)
    fsdf.printSchema()
  }







}
