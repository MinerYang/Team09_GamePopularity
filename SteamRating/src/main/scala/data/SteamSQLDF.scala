package data

import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.functions._

case object SteamSQLDF {
  lazy val appName = "SteamDataCleansing"
  lazy val master = "local[*]"

  def main(args: Array[String]) {
    val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
    ss.sparkContext.setLogLevel("WARN")
    import ss.implicits._

    //appid	name	release_date	english	developer	publisher	platforms	required_age	categories
    //genres	steamspy_tags	achievements	positive_ratings	negative_ratings	average_playtime	median_playtime	owners	price
    val schema = new StructType(Array(
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

    val df: DataFrame = ss.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .schema(schema)
      .load("Steam.csv")

    val tableNaDropped = df.na.drop()
    //    val df: DataFrame = ss.read.format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
    //      .load("Steam.csv")
    //    df.show(5)

    val table = initTable(tableNaDropped)
    //    table.printSchema()
    //    table.where("ratings = 0").show()

    val dfPrice = priceETS().minMaxSca(table, "price")
    val dfPrice1 = priceETS().chiSqSelector(dfPrice, "price_features", "ratings", 0.01)
    dfPrice1.show()
    print(dfPrice1.count())

    //    val df1 = PlatformETS().extractAndSelectFpr(table, "platforms", "ratings", 0.01)
    //      .withColumn("platforms_features", sparseToDense($"platforms_features"))
    //
    //    val df2 = categoriesETS().extractAndSelectFpr(df1, "categories", "ratings", 0.01)
    //
    //    val df3 = tagsETS().extractAndSelectFpr(df2, "tags", "ratings", 0.01)
    //
    //    val df4 = developerETS().extractAndSelectFpr(df3, "developer", target = "ratings", para = 0.01)
    //
    //    val df5 = publisherETS().extractAndSelectFpr(df4, "publisher", target = "ratings", para = 0.01)
    //
    //    df5.show()
    ss.stop()
  }

  def vecToArray = udf((v: Vector) => v.toArray)

  def doubleToVector = udf((ddd: Double) => Vectors.dense(ddd))

  def sparseToDense = udf((v: Vector) => v.toDense)

  def denseToSparse = udf((v: Vector) => v.toSparse)

  def getRawTable(df: DataFrame): DataFrame = df
    .withColumn("developer", split(df("developer"), ";"))
    .withColumn("publisher", split(df("publisher"), ";"))
    .withColumn("platforms", split(df("platforms"), ";"))
    .withColumn("categories", split(df("categories"), ";"))
    .withColumn("steamspy_tags", split(df("steamspy_tags"), ";"))
    .withColumnRenamed("steamspy_tags", "tags")
    .drop("name", "release_date", "english", "achievements", "genres",
      "required_age", "average_playtime", "median_playtime", "owners")

  def processRatings(df: DataFrame): DataFrame = {
    //TODO: maybe total=0?
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

  def processPrice(df: DataFrame): DataFrame = {
    df.withColumn("price", doubleToVector(df("price")))
  }

  def initTable(df: DataFrame): DataFrame = processPrice(processRatings(getRawTable(df)))
}

