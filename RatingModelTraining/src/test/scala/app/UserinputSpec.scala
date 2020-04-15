package app

import app.UserInput_Prediction.UserInput
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}


class UserinputSpec extends FlatSpec with Matchers{
  lazy val appName = "realTimePrediction"
  lazy val master = "local[*]"
  val ss = SparkSession.builder.master(master).appName(appName).getOrCreate()
  ss.sparkContext.setLogLevel("WARN")
  import ss.implicits._


  behavior of "processPrice"
  it should "scale price into a number between 0 and 1" in {
    val testdf = Seq(
      ("0",2.0),
      ("1",430.0),
    ).toDF("id","price")
    testdf.show()
    val res = UserInput_Prediction.processPrice(testdf)
    res.show()
    val list = res.select("price").rdd.map(f => f(0)).collect()
    for (i <- 0 to (list.length - 1)) {
      assert(list(i).asInstanceOf[Double] >= 0.0 && list(i).asInstanceOf[Double] <= 1.0)
    }
  }

}
