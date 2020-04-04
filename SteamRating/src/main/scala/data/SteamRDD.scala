package data


import data.SteamSQLDF.sparseToDense
import org.apache.spark.ml.feature.{ChiSqSelector, VectorAssembler}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types.{ArrayType, StringType, StructType}
import org.apache.spark.{SparkConf, SparkContext}

import scala.util._

case object SteamRDD {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
          .master("local[1]")
          .appName("test")
          .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.format("com.databricks.spark.csv")
      .option("header", "true") //在csv第一行有属性"true"，没有就是"false"
      .option("inferSchema", true.toString) //这是自动推断属性列的数据类型
      .load("/Users/mineryang/Desktop/steam-store-games/EDAdata.csv")

    val df1 = DataIngest.MergeToVec(df, "level")
    DataIngest.chiSqSelect(df1,50,"features","level")
   }


  }
