import org.apache.spark.sql.functions.when
import org.apache.spark.sql.{DataFrame, SparkSession}

case object SteamSQLDF {
  var appName = "SteamDataCleansing"
  var master = "local[*]"

  def main(args: Array[String]) {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()

    val df: DataFrame = ss.read.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("Steam.csv")

    //    df.show(5)

    val total_ratings = df("positive_ratings") + df("negative_ratings")
    val r = df("positive_ratings") / total_ratings

    val rawTable = df.withColumn("positive_ratings", when(r >= 0.95, 5)
      .when(r >= 0.8, 4)
      .when(r >= 0.7, 3)
      .when(r >= 0.4, 2)
      .when(r >= 0.2, 1)
      .otherwise(0))// ratings to categorical type
      .withColumnRenamed("positive_ratings", "ratings")
      .drop("name", "release_date", "english", "achievements", "negative_ratings", "genres",
        "required_age", "average_playtime", "median_playtime", "owners")

    rawTable.where("ratings = 0").show()

  }
}
