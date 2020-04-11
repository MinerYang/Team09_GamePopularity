package app

import ML._
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.{ChiSqSelector, CountVectorizer, MinMaxScaler, VectorAssembler}
import org.apache.spark.sql.{DataFrame, SparkSession}


object Stages_constructor {
  lazy val appName = "stagesConstruct"
  lazy val master = "local[*]"
  lazy val threshold = 0.05

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

    // import cleaned and preprocessed dataset
    val path = "/Users/mineryang/Documents/Team09_GamePopularity-JiaaoYu-working/RatingModelTraining/predata2.parquet"
    val origindf: DataFrame = ss.read.parquet(path)
    origindf.show(5)
    origindf.printSchema()


    // start build pipeline
    val target = "ratings"

    //for price
    val t1 = minMaxScaler("price")
    val df1: DataFrame = t1.fit(origindf).transform(origindf)
    val t2 = chiSqSelector("fpr", "price_features", target, threshold)
    val df2: DataFrame = t2.fit(df1).transform(df1)
    println("DF2")
    df2.printSchema()

    //for Platform, Categories, Tags, Developer, Publisher
    val t3 = countVectorize("platforms")
    val df3: DataFrame = t3.fit(df2).transform(df2)
    println("DF3")
    df3.printSchema()
    val t4 = chiSqSelector("fdr", "platforms_features", target, threshold)
    val df4: DataFrame = t4.fit(df3).transform(df3)
    println("DF4")
    df4.printSchema()

    val t5 = countVectorize("categories")
    val df5: DataFrame = t5.fit(df4).transform(df4)
    println("DF5")
    df5.printSchema()
    val t6 = chiSqSelector("fdr", "categories_features", target, threshold)
    val df6: DataFrame = t6.fit(df5).transform(df5)
    println("DF6")
    df6.printSchema()

    val t7 = countVectorize("tags")
    val df7: DataFrame = t7.fit(df6).transform(df6)
    val t8 = chiSqSelector("fdr", "tags_features", target, threshold)
    val df8: DataFrame = t8.fit(df7).transform(df7)

    val t9 = countVectorize("developer")
    val df9: DataFrame = t9.fit(df8).transform(df8)
    val t10 = chiSqSelector("fdr", "developer_features", target, threshold)
    val df10: DataFrame = t10.fit(df9).transform(df9)

    val t11 = countVectorize("publisher")
    val df11: DataFrame = t11.fit(df10).transform(df10)
    val t12 = chiSqSelector("fdr", "publisher_features", target, threshold)
    val df12: DataFrame = t12.fit(df11).transform(df11)
    println("DF12")
    df12.show()
    df12.printSchema()

    // for features
    val fs = featuresAssemb
    val fsdf: DataFrame = fs.transform(df12)
    println("fs_DF")
    fsdf.show()
    fsdf.printSchema()

    ////    stagesCollector.productIterator.foreach{ i =>println(i.toString)}

    //construct estimator
    /*  4 models*/
    val ml = {
      val lrMl = lr(fsdf)
      val rfMl = rf(fsdf)
      val mlpMl = mlp(fsdf)
      val nbMl = nb(fsdf)
      val candidate = List(lrMl, rfMl, mlpMl, nbMl)
      val accs = for (m <- candidate) yield{
        val predictions = m.transform(origindf)

        //     Select (prediction, true label) and compute test error
        val evaluator = new MulticlassClassificationEvaluator()
          .setLabelCol("label")
          .setPredictionCol("prediction")
          .setMetricName("accuracy")
        val accuracy = evaluator.evaluate(predictions)
        println(s"Test set accuracy = $accuracy")
        accuracy
      }
      val selected:Int = accs.zipWithIndex.maxBy(_._1)._2
      candidate(selected)
    }

    // construct stages with transformers ,estimator
    val stages = Array(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, fs, ml)

    //construct pipeline
    val pipeline = new Pipeline()
      .setStages(stages)

    //model traning
    //    val Array(pltrainingSet, pltestSet) = origindf.randomSplit(Array[Double](0.7, 0.3), 7777L)
    val pipelineModel = pipeline.fit(origindf)

    /**
     * save model locally
     */
    val exportpath = s"/Users/mineryang/Documents/Team09_GamePopularity-JiaaoYu-working/RatingModelTraining/selected_model"
    pipelineModel.write.overwrite().save(exportpath)
  }


  def minMaxScaler(colName: String): MinMaxScaler = {
    println("*** init a MinMaxScaler with column" + colName)
    val scaler = new MinMaxScaler()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")
    scaler
  }

  def chiSqSelector(stype: String, colName: String, target: String, para: Double): ChiSqSelector = {
    println("*** init a ChiSqSelector type:" + stype + " with column: " + colName)
    if (stype.equals("fpr")) {
      val selector = new ChiSqSelector()
        .setSelectorType("fpr")
        .setFpr(para)
        .setFeaturesCol(colName)
        .setLabelCol(target)
        .setOutputCol("selected_" + colName)
      return selector
    }
    else if (stype.equals("fdr")) {
      val selector = new ChiSqSelector()
        .setSelectorType("fdr")
        .setFdr(para)
        .setFeaturesCol(colName)
        .setLabelCol(target)
        .setOutputCol("selected_" + colName)
      return selector
    }
    println("no selector type defined")
    val selector = new ChiSqSelector()
    selector
  }

  def countVectorize(colName: String): CountVectorizer = {
    println("*** init a CountVectorizer with column: " + colName)
    val cv = new CountVectorizer()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")
      .setMinDF(2)
    cv
  }

  def featuresAssemb: VectorAssembler = {
    println("*** init a VectorAssembler")
    val featureCol = Array(
      "selected_price_features",
      "selected_platforms_features",
      "selected_categories_features",
      "selected_tags_features",
      "selected_developer_features",
      "selected_publisher_features"
    )
    val assembler = new VectorAssembler()
      .setInputCols(featureCol)
      .setOutputCol("features")
    assembler
  }


  def stagesCollector(): Tuple13[
    MinMaxScaler, ChiSqSelector,
    CountVectorizer, ChiSqSelector,
    CountVectorizer, ChiSqSelector,
    CountVectorizer, ChiSqSelector,
    CountVectorizer, ChiSqSelector,
    CountVectorizer, ChiSqSelector,
    VectorAssembler] = {
    val target = "ratings"

    //for price
    val t1 = minMaxScaler("price")
    val t2 = chiSqSelector("fpr", "price", target, threshold)

    //for Platform, Categories, Tags, Developer, Publisher
    val t3 = countVectorize("platforms")
    val t4 = chiSqSelector("fdr", "platforms", target, threshold)

    val t5 = countVectorize("categories")
    val t6 = chiSqSelector("fdr", "categories", target, threshold)

    val t7 = countVectorize("tags")
    val t8 = chiSqSelector("fdr", "tags", target, threshold)

    val t9 = countVectorize("developer")
    val t10 = chiSqSelector("fdr", "developer", target, threshold)

    val t11 = countVectorize("publisher")
    val t12 = chiSqSelector("fdr", "publisher", target, threshold)

    // for features
    val fs = featuresAssemb

    //contruct tuple
    val tp = (t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, fs)
    tp
  }

}
