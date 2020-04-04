package data

import org.apache.spark.ml.linalg.{Vector,Vectors}
import org.scalatest.{FlatSpec, Matchers}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import scala.util._

class DataIngestSpec extends FlatSpec with Matchers{
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Unit test")
    .getOrCreate()

  import spark.implicits._


  behavior of  "colParse"
  it should "work" in {
    val testdf = Seq(
      ("1", "Sports;Action;Adventure"),
      ("2", "Action;Casual")
    ).toDF("id","tags")
    val std = Seq(
      ("1", Array("Sports", "Action", "Adventure")),
      ("2", Array("Action", "Casual"))
    ).toDF("id","tags")
    val res = DataIngest.colParse(testdf, "tags")
    res.schema.equals(std.schema) shouldBe true
    res.collect().sameElements(std.collect()) shouldBe true
  }

  behavior of "MergeToVec"
  it should "work" in {
    val testdf = Seq(
      (false, true, false, 1),
      (true, true, false, 2),
      (false, false, true, 3)
    ).toDF("t1","t2","t3","price_level")

//    var cola = testdf.columns
//    cola = cola.filter(! _.contains("price_level"))
    val res = DataIngest.MergeToVec(testdf,"price_level")
    res.show(false)
    res.printSchema()
    val std = Seq(
      (false, true, false, 1, Vectors.dense(0.0,1.0,0.0,1.0)),
      (true, true, false, 2, Vectors.dense(1,1,0,2)),
      (false, false, true, 3, Vectors.dense(0.0,0.0,1.0,3.0))
    )toDF("t1","t2","t3","price_level", "features")
    std.show(false)
    std.printSchema()
//
////    res.schema.equals(std.schema) shouldBe true
    res.collect().sameElements(std.collect()) shouldBe true
  }

}
