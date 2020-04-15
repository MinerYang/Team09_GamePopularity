package app

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.functions.{split, udf, when}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

case object DataCleaning {
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
    val path1 = "./src/main/resources/steam.csv"
    val df: DataFrame = ss.read.format("org.apache.spark.csv")
      .option("header", "true")
      .schema(schema)
      .option("dateFormat", "m/d/YYYY")
      .csv(path1)
    df.show()

    val df_clean = cleanData(df)
    val df_pre = preprocess(df_clean)
    df_pre.show(5)
    df_pre.printSchema()

    //save clean dataset
    val path2 = "./cleandata.parquet"
    df_pre.write.option("mergeSchema","true").parquet(path2)
    val parquetFileDF = ss.read.parquet(path2)
    parquetFileDF.show(5)
  }

//  def doubleToVector = udf((ddd: Double) => Vectors.dense(ddd))

  def cleanData(df: DataFrame): DataFrame = df.na.drop()

  /**
   * pre-chosen and process columns
   *
   * @param df
   * @return DataFrame
   */
  def preprocess(df: DataFrame): DataFrame = {
//    val df1 = processPrice(df)
    val df2 = processRatings(df)
    parseData(df2)
  }


//  def processPrice(df: DataFrame): DataFrame = {
//    df.withColumn("price", doubleToVector(df("price")))
//  }

  /**
   * calculate and categorize our label from positive_ratings and negative_ratings
   *
   * @param df
   * @return
   */
  def processRatings(df: DataFrame): DataFrame = {

    df.withColumn("positive_ratings", df("positive_ratings").cast("Decimal"))
      .withColumn("negative_ratings", df("negative_ratings").cast("Decimal"))
      .withColumnRenamed("positive_ratings", "p")
      .withColumnRenamed("negative_ratings", "n")
      .createOrReplaceTempView("temp")
    // Lower bound of Wilson score confidence interval for a Bernoulli parameter
    val rank = df.sqlContext.sql("SELECT appid,name, " +
      "((p + 1.9208) / (p + n) - 1.96 * SQRT((p * n) / (p + n) + 0.9604) / (p + n)) / (1 + 3.8416 / (p + n)) " +
      "AS cilb FROM temp WHERE p + n > 0 ")
    rank.printSchema()
    //      + "ORDER BY cilb DESC")
    //    rank.show()

    rank.createOrReplaceTempView("rank")
    val percent70: Double = df.sqlContext.sql("SELECT percentile(cilb, 0.70) FROM rank").first().getAs[Double](0)
    val percent80: Double = df.sqlContext.sql("SELECT percentile(cilb, 0.80) FROM rank").first().getAs[Double](0)
    val percent90: Double = df.sqlContext.sql("SELECT percentile(cilb, 0.90) FROM rank").first().getAs[Double](0)
    val percent95: Double = df.sqlContext.sql("SELECT percentile(cilb, 0.95) FROM rank").first().getAs[Double](0)
    df.createOrReplaceTempView("temp")
    df.printSchema()
//    val dfr = df.sqlContext.sql("SELECT * FROM temp JOIN rank ON temp.appid = rank.appid")
    //avoid duplicate column with index
    val dfr = df.join(rank,"appid")
    dfr.printSchema()
    dfr.withColumn("ratings", when(dfr("cilb") >= percent95, 4.0)
      .when(dfr("cilb") >= percent90, 3.0)
      .when(dfr("cilb") >= percent80, 2.0)
      .when(dfr("cilb") >= percent70, 1.0)
      .otherwise(0.0))
      .drop("positive_ratings", "negative_ratings", "cilb")
  }


  /**
   * parse some columns
   *
   * @param df
   * @return DataFrame
   */
  def parseData(df: DataFrame): DataFrame = df
    .withColumn("developer", split(df("developer"), ";"))
    .withColumn("publisher", split(df("publisher"), ";"))
    .withColumn("platforms", split(df("platforms"), ";"))
    .withColumn("categories", split(df("categories"), ";"))
    .withColumn("steamspy_tags", split(df("steamspy_tags"), ";"))
    .withColumnRenamed("steamspy_tags", "tags")
    .drop("name", "release_date", "english", "achievements", "genres",
      "required_age", "average_playtime", "median_playtime", "owners")
}
