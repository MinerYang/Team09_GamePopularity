package app

import app.ML._
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{DataFrame, SparkSession}
import MachineLearning.PipelineTransfomer._
import org.apache.spark.ml.classification.{LogisticRegression, NaiveBayes, NaiveBayesModel}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import app.DataCleaning._

import scala.reflect.io.File


object ModelExport {
  lazy val appName = "stagesConstruct"
  lazy val master = "local[*]"
  lazy val threshold = 0.05

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    val path = "/Users/mineryang/Desktop/Team09_GamePopularity/RatingModelTraining"
    val origindf = ss.read.parquet(s"$path/cleandata.parquet")
    val rawdf = readcsv(ss)

    /**
     * chose functions you want to conduct
     * 3 kinds : trainModelToExport, saveFeature locally, and saveToCsv
     */
    printHint()
    while(true){
      val option = scala.io.StdIn.readLine()
      option match {
        case "1" => trainModelToExport(origindf, path)
        case "2" => saveFeatureProcess(origindf, path)
        case "3" => saveToCsv(rawdf)
        case "4" => savePipeline(path)
        case "0" => sys.exit()
        case _ => println("invalid option, please type again")
      }
    }

  }


  def printHint() = {
    println("**********************************************")
    println("please choose the function you want to conduct\n" +
      "1.Training model to Export\n"+
      "2.Save selected features locally\n"+
      "3.Save testdata to csv file locally\n"+
      "4.Save useable Pipeline locally\n"+
      "0.Process exit")
  }


  /**
   * Traning model and export
   * @param df
   * @param path
   */
  def trainModelToExport (df:DataFrame,path:String) ={
    /**
     * choose one of the best training model to export
     */
    val name="NB"
    val nb = new NaiveBayes()
//  construct new pipeline for web use
    val stages = Array(indexer,t0,t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, fs, nb)
    val pipeline = new Pipeline()
      .setStages(stages)
    print("Completed : 70 %\n")
    // start training
    val Array(trainingSet, testSet) = df.randomSplit(Array[Double](0.7, 0.3), 500)
    val pipelineModel = pipeline.fit(trainingSet)
    println("model training complete")
    evaluation(pipelineModel,testSet)
    saveModel(pipelineModel,path)
    println("model export complete")
    printHint()
  }


  /**
   * save a  NaiveBayes pipeline locally for futher use
   */
  def savePipeline(path:String) = {
    val pipeline = initNBpipeline()
    val export = s"$path/my_pipeline"
    pipeline.write.overwrite().save(export)
    println("This NaiveBayes pipeline has saved for further use")
    printHint()
  }

  def initNBpipeline():Pipeline ={
    val nb = new NaiveBayes()
    val stages = Array(indexer,t0,t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, fs, nb)
    val pipeline = new Pipeline().setStages(stages)
    return pipeline
  }

  /**
   * save model locally
   */
  def saveModel(model: PipelineModel,path:String): Unit ={
    val exportpath = s"$path/best_model"
    model.write.overwrite().save(exportpath)
    println("This model has saved for further use")
  }

  def evaluation(model: PipelineModel,testdf:DataFrame): Unit ={
    val predictions = model.transform(testdf)
    predictions.select("ratings","label","prediction", "probability").show(5)
    val evaluator1 = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator1.evaluate(predictions)
    println(s"Test set accuracy = $accuracy")
  }

  /**
   * save selected features for further use
   * printout featuring process for each transformers
   * nothing to do with pipeline training
   */
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
    println("all selected features has saved locally ")
    printHint()
  }


  /**
   * save clean test data into csv file for further use
   * @param rawdf
   */
  def saveToCsv(rawdf:DataFrame) = {
    val df0 = processRatings(rawdf)
    val df = selectcolumns(df0)
//    df.write.format("csv").save(s"$path/testdata1")
     df.write.mode("overwrite").option("header", "true").csv(s"/Users/mineryang/Desktop/testdata")
    println("test data exported to local csv file")
    printHint()
  }

  def selectcolumns(df:DataFrame):DataFrame = {
    df.select("appid","developer","publisher","platforms","categories","steamspy_tags","price","ratings")
      .withColumnRenamed("steamspy_tags","tags")
      .na.drop()
  }

  def readcsv(ss:SparkSession ):DataFrame = {
    val schema = new StructType(Array
    (
      StructField("appid", DataTypes.IntegerType),
      StructField("name", DataTypes.StringType),
      StructField("release_date", DataTypes.DateType),
      StructField("english", DataTypes.IntegerType),
      StructField("developer", DataTypes.StringType),
      StructField("publisher", DataTypes.StringType),
      StructField("platforms", DataTypes.StringType),
      StructField("required_age", DataTypes.IntegerType),
      StructField("categories", DataTypes.StringType),
      StructField("genres", DataTypes.StringType),
      StructField("steamspy_tags", DataTypes.StringType),
      StructField("achievements", DataTypes.IntegerType),
      StructField("positive_ratings", DataTypes.IntegerType),
      StructField("negative_ratings", DataTypes.IntegerType),
      StructField("average_playtime", DataTypes.IntegerType),
      StructField("median_playtime", DataTypes.IntegerType),
      StructField("owners", DataTypes.StringType),
      StructField("price", DataTypes.DoubleType),
    ))
    val path1 = "/Users/mineryang/Desktop/Team09_GamePopularity-JiaaoYu-working/SteamRating/steam.csv"
    val df: DataFrame = ss.read.format("org.apache.spark.csv")
      .option("header", "true")
      .schema(schema)
      .option("dateFormat", "m/d/YYYY")
      .csv(path1)
    df
  }







}
