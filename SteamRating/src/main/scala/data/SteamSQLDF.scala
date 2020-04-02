package data

import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

case object SteamSQLDF {
  lazy val appName = "SteamDataCleansing"
  lazy val master = "local[*]"

  def vecToArray = udf((v: Vector) => v.toArray)

  def getRawTable(df: DataFrame): DataFrame = df.withColumn("platforms", split(df("platforms"), ";"))
    .withColumn("categories", split(df("categories"), ";"))
    .withColumn("steamspy_tags", split(df("steamspy_tags"), ";"))
    .drop("name", "release_date", "english", "achievements", "genres",
      "required_age", "average_playtime", "median_playtime", "owners")

  def processRatings(df: DataFrame): DataFrame = {
    val total_ratings = df("positive_ratings") + df("negative_ratings")
    val r = df("positive_ratings") / total_ratings
    df.withColumn("positive_ratings", when(r >= 0.95, 5)
      .when(r >= 0.8, 4)
      .when(r >= 0.7, 3)
      .when(r >= 0.4, 2)
      .when(r >= 0.2, 1)
      .otherwise(0)) // ratings to categorical type
      .withColumnRenamed("positive_ratings", "ratings")
      .drop("negative_ratings")
  }

  def main(args: Array[String]) {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    import ss.implicits._

    val df: DataFrame = ss.read.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("Steam.csv")
    //    df.show(5)

    val rawTable = processRatings(getRawTable(df))
    //    rawTable.where("ratings = 0").show()

    val df1 = new PlatformETS().extract(rawTable, "platforms")
      .withColumn("platforms_features", vecToArray($"platforms_features"))

    df1.show()
  }
}

