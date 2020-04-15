import MachineLearning.PipelineTransfomer
import MachineLearning.PipelineTransfomer.{frameTransform, printselect, t7, t8}
import app.ModelExport
import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel, CountVectorizerModel}
import org.scalatest.{FlatSpec, Matchers}


class PipelineSpec  extends FlatSpec with Matchers{

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

  behavior of "minMaxScaler"
  it should "init a MinMaxScaler with column" in {
    val exp = PipelineTransfomer.minMaxScaler("test")
    exp.getInputCol shouldBe("test")
    exp.getOutputCol shouldBe("test" + "_features")
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
