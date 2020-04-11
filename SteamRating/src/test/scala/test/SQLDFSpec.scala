package test

import Schema.GameSchema
import data.SteamSQLDF
import data.SteamSQLDF.{appName, master}
import org.apache.spark.sql.types.{DoubleType, IntegerType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}


class SQLDFSpec extends FlatSpec with Matchers{

  lazy val test = getClass.getResource("/test.csv").getPath
  val ss = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
  ss.sparkContext.setLogLevel("WARN")
  import ss.implicits._
  val df: DataFrame = ss.read.format("org.apache.spark.csv")
    .option("header", "true")
    .schema(GameSchema.schema)
    .option("dateFormat", "m/d/YYYY")
    .csv("D:\\Idea Proj\\Team09_GamePopularity\\SteamRating\\src\\test\\resources")


  behavior of "getRawTable"
  it should "Get the raw table with 9 cols and with certain col names" in {
    val rawdf = SteamSQLDF.getRawTable(df)
    assert(rawdf.columns.length == 9)
    //rawdf.schema.fields.map(f =>f.name).toList
  }

  behavior of "processRatings"
  it should "pre-process ratings which is a double val and less than 1" in {
    val prdf = SteamSQLDF.processRatings(df)
    prdf.select("ratings").rdd.map(f => f(0)).collect.foreach(a => a should matchPattern{
      case 0.0 =>
      case 1.0 =>
      case 2.0 =>
    })

  }

  behavior of "processPrice"
  it should "get a df of price" in {
    val ppdf = SteamSQLDF.processPrice(df)
    val list = ppdf.schema.fields.map(f => f.dataType).toList
    assert(list.last != DoubleType)
  }
}
