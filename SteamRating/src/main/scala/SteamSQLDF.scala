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

    val pRatings = df("positive_ratings")
    val nRatings = df("negative_ratings")
    val total_ratings = pRatings + nRatings
    val ratings = ((pRatings / total_ratings)*100).cast("int")
    val df1 = df.withColumn("positive_ratings", ratings).withColumnRenamed("positive_ratings", "ratings")
    val df2 = df1.drop("name", "release_date", "english", "achievements", "negative_ratings", "genres",
      "required_age", "average_playtime", "median_playtime", "owners")
    df2.show(5)
  }
}
