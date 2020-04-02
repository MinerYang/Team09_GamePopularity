package data

import org.apache.spark.ml.feature.{ChiSqSelector, CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.DataFrame
//Column Feature Extractor,Transformer,Selector
trait ColumnFeatureETS {

  def extract(df: DataFrame, colName:String): DataFrame = {
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol(colName)
      .setOutputCol("features")
      .setMinDF(2)
      .fit(df)
    cvModel.transform(df)
  }

  def chiSqSelect(a: DataFrame, NumTopFeatures: Int) = {
    val selector = new ChiSqSelector()
      .setNumTopFeatures(NumTopFeatures)
      .setFeaturesCol("features")
      .setLabelCol("ratings")
      .setOutputCol("selectedFeatures")

    val result = selector.fit(a).transform(a)

    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result.show()
  }

}

case class PlatformETS() extends ColumnFeatureETS {
}

case object PlatformETS {
}