import app.{DataCleaning, UserInput_Prediction}
import app.UserInput_Prediction.{UserInput, appName, master, parseData, processPrice}
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DoubleType
import org.scalatest.{FlatSpec, Matchers}


class UserinputSpec extends FlatSpec with Matchers{
  lazy val appName = "realTimePrediction"
  lazy val master = "local[*]"
  val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
  ss.sparkContext.setLogLevel("WARN")
  val path = "."
  val loadmodel = PipelineModel.load(s"$path/best_model")
  val dev: String = "Valve"
  val pub: String = "Valve"
  val plt: String = "windows;mac;linux"
  val cat: String = "Multi-player;Online Multi-Player;Local Multi-Player;Valve Anti-Cheat enabled"
  val tagss: String = "Action;FPS;Multiplayer"

  val price: Double = 7.19
  val inputdata = Array(UserInput(dev, pub, plt, cat, tagss, price))
  val userInputRDD = ss.sparkContext.parallelize(inputdata)
  val testdf = ss.sqlContext.createDataFrame(userInputRDD)

  behavior of "parseData"
  it should "Get the raw table with 9 cols and with certain col names" in {
    val pd_df = UserInput_Prediction.parseData(testdf)
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

  behavior of "processPrice"
  it should "get a df of price" in {
    val df1 = UserInput_Prediction.parseData(testdf)
    val ppdf = UserInput_Prediction.processPrice(df1)
    val list = ppdf.select("price").rdd.map(f => f(0)).collect()
    for (i <- 0 to (list.length - 1)) {
      assert(list(i).asInstanceOf[Int] >= 0 && list(i).asInstanceOf[Int] <= 1)
    }
  }

}
