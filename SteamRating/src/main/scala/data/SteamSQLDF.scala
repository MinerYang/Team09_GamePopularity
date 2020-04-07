package data

import java.io.File

import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.linalg.{SparseVector, Vector, Vectors}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, _}
import org.apache.spark.sql.{DataFrame, SparkSession}

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
    //      .format("com.databricks.spark.csv")
    val df: DataFrame = ss.read.format("org.apache.spark.csv")
      .option("header", "true")
      .schema(schema)
      .option("dateFormat", "m/d/YYYY")
      .csv("hdfs://localhost:9000/steam.csv")
    df.printSchema()
    df.show()
    val tableNaDropped = df.na.drop()
    //    val df: DataFrame = ss.read.format("com.databricks.spark.csv")
    //      .option("header", "true")
    //      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
    //      .load("Steam.csv")
    //    df.show(5)

    val table = initTable(tableNaDropped)
    //    table.printSchema()
    //    table.where("ratings = 0").show()

    val dfP = priceETS().minMaxSca(table, "price")
    //    val df1 = table.select(concat_ws(",", $"developer", $"publisher", $"platforms", $"categories", $"tags").cast(StringType).as("features"))

    //    val result = EtsHelper().extractAndSelectFpr(df2, "features", "ratings", 0.01)

    val dfPrice = priceETS().chiSqSelectorFpr(dfP, "price_features", "ratings", 0.01)
    val dfPlat = PlatformETS().extractAndSelectFpr(dfPrice, "platforms", "ratings", 0.01)
    val dfCate = categoriesETS().extractAndSelectFpr(dfPlat, "categories", "ratings", 0.01)
    val dfTag = tagsETS().extractAndSelectFpr(dfCate, "tags", "ratings", 0.01)
    val dfDev = developerETS().extractAndSelectFpr(dfTag, "developer", target = "ratings", para = 0.01)
    val dfPub = publisherETS().extractAndSelectFpr(dfDev, "publisher", target = "ratings", para = 0.01)

    val dfAssembled = EtsHelper().vectorAss(dfPub, "Features")
    dfAssembled.show()

    val labeled = dfAssembled.map(row => LabeledPoint(row.getAs[Double]("ratings"),
      row.getAs[Vector]("features")))

    labeled.write.format("libsvm").save("hdfs://localhost:9000/123.txt")

    ss.stop()
  }

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
    df.withColumn("positive_ratings", when(r >= 0.95, 5.0)
      .when(r >= 0.8, 4.0)
      .when(r >= 0.7, 3.0)
      .when(r >= 0.4, 2.0)
      .when(r >= 0.2, 1.0)
      .otherwise(0.0)) // ratings to categorical type
      .withColumnRenamed("positive_ratings", "ratings")
      .drop("negative_ratings")
  }

  def processPrice(df: DataFrame): DataFrame = {
    df.withColumn("price", doubleToVector(df("price")))
  }

  def initTable(df: DataFrame): DataFrame = processPrice(processRatings(getRawTable(df)))

  def vecToArray = udf((v: Vector) => v.toArray)

  def doubleToVector = udf((ddd: Double) => Vectors.dense(ddd))

  def sparseToDense = udf((v: Vector) => v.toDense)

  def denseToSparse = udf((v: Vector) => v.toSparse)

}

