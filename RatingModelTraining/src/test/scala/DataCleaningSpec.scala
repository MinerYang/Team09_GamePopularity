import app.DataCleaning
import app.DataCleaning.{cleanData, preprocess}
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
    .csv("./src/test/resources/test.csv")
  val df_clean = cleanData(df)
  val df_pre = preprocess(df_clean)



  behavior of "parseData"
  it should "Get the raw table with 9 cols and with certain col names" in {
    val pd_df = DataCleaning.parseData(df)
    pd_df.show
    assert(pd_df.columns.length == 9)
    pd_df.columns.foreach(a => a should matchPattern{
      case "appid" =>
      case "developer" =>
      case "publisher" =>
      case "platforms" =>
      case "categories" =>
      case "tags" =>
      case "positive_ratings" =>
      case "negative_ratings" =>
      case "price" =>
    })
  }

  behavior of "processRatings"
  it should "pre-process ratings which is a double val and less than 1" in {
    val prdf = DataCleaning.processRatings(df_clean)
    prdf.select("ratings").rdd.map(f => f(0)).collect.foreach(a => a should matchPattern{
      case 0.0 =>
      case 1.0 =>
      case 2.0 =>
      case 3.0 =>
      case 4.0 =>
    })
  }
}
