package MachineLearning

import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.scalatest.{FlatSpec, Matchers}


class PipelineSpec  extends FlatSpec with Matchers{
  val spark: SparkSession = SparkSession.builder()
    .master("local[1]")
    .appName("Unit test")
    .getOrCreate()

  import spark.implicits._

  behavior of "labelIndex"
  it should "init a StringIndexer for colName" in {
    val exp = PipelineTransfomer.labelIndex("test")
    exp.getInputCol shouldBe("test")
    exp.getOutputCol shouldBe("label")
  }

  behavior of "priceVector"
  it should "init a VectorAssembler" in {
    val exp = PipelineTransfomer.priceVector("test")
    exp.getInputCols shouldBe(Array("test"))
    exp.getOutputCol shouldBe("price_vec")
  }
  it should "work" in {
    val testdf = Seq(
      (false, true, false, 1),
      (true, true, false, 2),
      (false, false, true, 3)
    ).toDF("t1","t2","t3","price")
    val res =  PipelineTransfomer.priceVector("price").transform(testdf)
    res.printSchema()
    val std = Seq(
      (false, true, false, 1, Vectors.dense(1)),
      (true, true, false, 2, Vectors.dense(2)),
      (false, false, true, 3, Vectors.dense(3))
    )toDF("t1","t2","t3","price","price_vec")
    std.printSchema()
    //    res.schema.equals(std.schema) shouldBe true
    res.collect().sameElements(std.collect()) shouldBe true
  }

  behavior of "minMaxScaler"
  it should "init a MinMaxScaler with column" in {
    val exp = PipelineTransfomer.minMaxScaler("test")
    exp.getInputCol shouldBe("test")
    exp.getOutputCol shouldBe("test" + "_features")
  }
  it should "work" in {
    val testdf = Seq(
      (false, true, false, Vectors.dense(1)),
      (true, true, false,  Vectors.dense(2)),
      (false, false, true, Vectors.dense(3))
    )toDF("t1","t2","t3","price_vec")
    val res = PipelineTransfomer.minMaxScaler("price_vec").fit(testdf).transform(testdf)
    val std = Seq(
      (false, true, false, Vectors.dense(1),Vectors.dense(0.0)),
      (true, true, false,  Vectors.dense(2),Vectors.dense(0.5)),
      (false, false, true, Vectors.dense(3),Vectors.dense(1.0))
    )toDF("t1","t2","t3","price_vec","price_vec_features")
    res.show()
    res.printSchema()
    res.schema.equals(std.schema) shouldBe true
    res.collect().sameElements(std.collect()) shouldBe true
  }

  behavior of "chiSqSelector"
  it should "init a ChiSqSelector with certain type" in {
    val exp = PipelineTransfomer.chiSqSelector("fpr", "test", "target", 1.0)
    exp.getSelectorType shouldBe("fpr")
    exp.getOutputCol shouldBe("selected_" + "test")
    exp.getLabelCol shouldBe("target")
    exp.getFpr shouldBe(1.0)
    val exp2 = PipelineTransfomer.chiSqSelector("fdr", "test", "target", 1.0)
    exp2.getSelectorType shouldBe("fdr")
    exp2.getOutputCol shouldBe("selected_" + "test")
    exp2.getLabelCol shouldBe("target")
    exp2.getFdr shouldBe(1.0)
    val exp3 = PipelineTransfomer.chiSqSelector("1q23", "test", "target", 1.0)
    exp3.getSelectorType shouldBe("numTopFeatures")
  }

  behavior of "countVectorize"
  it should "init a countVectorize with column" in {
    val exp = PipelineTransfomer.countVectorize("test")
    exp.getInputCol shouldBe("test")
    exp.getOutputCol shouldBe("test" + "_features")
  }

}
