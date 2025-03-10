package app

import app.DataCleaning._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.functions.{split, when}
import org.apache.spark.sql.{DataFrame, SparkSession}

object UserInput_Prediction {
  lazy val appName = "realTimePrediction"
  lazy val master = "local[*]"

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    // Load data
    val path = "./src/test/resources"
    val loadmodel = PipelineModel.load(s"$path/best_model")

    /**
     * This is to simulate some user input to do realtime predictions
     * developer array[String]
     * publisher array[String]
     * platform array[String]
     * categories array[String]
     * tags array[String]
     * price double
     */
    val dev: String = "Valve"
    val pub: String = "Valve"
    val plt: String = "windows;mac;linux"
    val cat: String = "Multi-player;Online Multi-Player;Local Multi-Player;Valve Anti-Cheat enabled"
    val tags: String = "Action;FPS;Multiplayer"
    val price: Double = 7.19

    val inputdata = Array(UserInput(dev, pub, plt, cat, tags, price))
    val userInputRDD = ss.sparkContext.parallelize(inputdata)

    // Print the RDD for debugging
    //    userInputRDD.collect().foreach(println)

    val testdf = ss.sqlContext.createDataFrame(userInputRDD)
    // testdf.printSchema()
    testdf.show()
    val df1 = parseData(testdf)
    val df2 = processPrice(df1)


    //transform a inputdataframe into a dataframe with features columns
    val prediction = loadmodel.transform(df2)
    prediction.select("prediction","probability")show()
  }

  def parseData(df: DataFrame): DataFrame = df
    .withColumn("developer", split(df("developer"), ";"))
    .withColumn("publisher", split(df("publisher"), ";"))
    .withColumn("platforms", split(df("platforms"), ";"))
    .withColumn("categories", split(df("categories"), ";"))
    .withColumn("tags", split(df("tags"), ";"))

  def processPrice(df:DataFrame):DataFrame = df.
    withColumn("price", when(df("price") < 421.99, df("price") / 421.99).otherwise(1))


  case class UserInput(developer: String,
                       publisher: String,
                       platforms: String,
                       categories: String,
                       tags: String,
                       price: Double,
                      )
}
