package data

import org.apache.spark.ml.feature.{ChiSqSelector, CountVectorizer, CountVectorizerModel}
import org.apache.spark.sql.DataFrame

//Column Feature Extractor,Transformer,Selector
trait ColumnFeatureETS {

  def extract(df: DataFrame, colName: String): DataFrame = {
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")
      .setMinDF(2)
      .fit(df)
    cvModel.transform(df)
  }

  def chiSqSelect(df: DataFrame, colName: String, target: String, NumTopFeatures: Int) = {
    val selector = new ChiSqSelector()
      .setNumTopFeatures(NumTopFeatures)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName + "_Features")

    val result = selector.fit(df)

    println(s"ChiSqSelector output with top ${selector.getNumTopFeatures} features selected")
    result.selectedFeatures foreach println
    result.transform(df).show()
  }

}

case class PlatformETS() extends ColumnFeatureETS {
}

case object PlatformETS {
}

case class categoriesETS() extends ColumnFeatureETS {
}

case object categoriesETS {
}
