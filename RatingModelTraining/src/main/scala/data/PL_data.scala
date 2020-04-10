package data

import org.apache
import org.apache.spark
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{split, udf, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

case object PL_data{
  lazy val appName = "SteamDataCleansing"
  lazy val master = "local[*]"
  lazy val threshold = 0.05

  def main(args: Array[String]): Unit = {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")

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

    val df: DataFrame = ss.read.format("org.apache.spark.csv")
      .option("header", "true")
      .schema(schema)
      .option("dateFormat", "m/d/YYYY")
      .csv("/Users/mineryang/Desktop/Team09_GamePopularity-JiaaoYu-working/SteamRating/steam.csv")
    df.show()

    val df_clean = cleanData(df)
    val df_pre = preprocess(df_clean)
    df_pre.show(5)
    df_pre.printSchema()

    //save clean dataset
    val path = "/Users/mineryang/Documents/Team09_GamePopularity-JiaaoYu-working/RatingModelTraining/predata2.parquet"
    df_pre.write.parquet(path)
    val parquetFileDF = ss.read.parquet(path)
    parquetFileDF.show(5)
  }

  def doubleToVector = udf((ddd: Double) => Vectors.dense(ddd))
  def cleanData(df:DataFrame): DataFrame = df.na.drop()
  /**
   *  pre-chosen and process columns
   * @param df
   * @return DataFrame
   */
  def preprocess(df:DataFrame) : DataFrame  = {
    val df1  = processPrice(df)
    val df2 = processRatings(df1)
    parseData(df2)
  }


  def processPrice(df: DataFrame): DataFrame = {
    df.withColumn("price", doubleToVector(df("price")))
  }

  /**
   * calculate and categorize our label from positive_ratings and negative_ratings
   * @param df
   * @return
   */
  def processRatings(df: DataFrame): DataFrame = {
    //TODO: maybe total=0?
    val t = df("positive_ratings") + df("negative_ratings")
    val r = df("positive_ratings") / t
    //    val tMean: Double = df.agg(mean(t)).as("mean").first().getAs[Double](0)
    //    val tStd: Double = df.agg(stddev(t)).as("std").first().getAs[Double](0)
    val tMed: Double = {
      df.withColumn("total", t).createOrReplaceTempView("total")
      val fewRev = df.sqlContext.sql("SELECT percentile(total, 0.67) FROM total").first().getAs[Double](0)
      println("if number of ratings is less than {" + fewRev + "} will be considered as FEW REVIEWS")
      fewRev
    }

    df.withColumn("positive_ratings", when(t < tMed, 0.0)
      .when(r >= 0.95, 4.0)
      .when(r >= 0.7, 3.0)
      .when(r >= 0.4, 2.0)
      .otherwise(1.0))
      .withColumnRenamed("positive_ratings", "ratings")
      .drop("negative_ratings", "total")
  }

  /**
   *  parse some columns
   * @param df
   * @return DataFrame
   */
  def parseData(df:DataFrame) :DataFrame = df
    .withColumn("developer", split(df("developer"), ";"))
    .withColumn("publisher", split(df("publisher"), ";"))
    .withColumn("platforms", split(df("platforms"), ";"))
    .withColumn("categories", split(df("categories"), ";"))
    .withColumn("steamspy_tags", split(df("steamspy_tags"), ";"))
    .withColumnRenamed("steamspy_tags", "tags")
    .drop("name", "release_date", "english", "achievements", "genres",
      "required_age", "average_playtime", "median_playtime", "owners")



}
