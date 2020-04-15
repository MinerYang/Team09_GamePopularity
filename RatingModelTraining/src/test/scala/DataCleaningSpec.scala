import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}
import schema.GameSchema


class DataCleaningSpec extends FlatSpec with Matchers{

  lazy val test = getClass.getResource("/test.csv").getPath
  val ss = SparkSession.builder.master("local[*]").appName("test").getOrCreate()
  ss.sparkContext.setLogLevel("WARN")
  val df: DataFrame = ss.read.format("org.apache.spark.csv")
    .option("header", "true")
    .schema(GameSchema.schema)
    .option("dateFormat", "m/d/YYYY")
    .csv("./src/test/resources/test.scv")


  behavior of "getRawTable"
  it should "Get the raw table with 9 cols and with certain col names" in {
    //val rawdf = SteamSQLDF.getRawTable(df)
    //assert(rawdf.columns.length == 9)
    //rawdf.schema.fields.map(f =>f.name).toList
  }

  behavior of "processRatings"
  it should "pre-process ratings which is a double val and less than 1" in {
//    val prdf = SteamSQLDF.processRatings(df)
//    prdf.select("ratings").rdd.map(f => f(0)).collect.foreach(a => a should matchPattern{
//      case 0.0 =>
//      case 1.0 =>
//      case 2.0 =>
//    })

  }

  behavior of "processPrice"
  it should "get a df of price" in {
//    val ppdf = SteamSQLDF.processPrice(df)
//    val list = ppdf.schema.fields.map(f => f.dataType).toList
//    assert(list.last != DoubleType)
//  }
}
