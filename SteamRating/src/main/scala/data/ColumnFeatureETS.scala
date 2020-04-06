package data
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.{MinMaxScaler, CountVectorizer, CountVectorizerModel, ChiSqSelector, ChiSqSelectorModel,VectorAssembler,LabeledPoint}


//Column Feature Extractor,Transformer,Selector
trait ColumnFeatureETS {
  def extractFeatures(df: DataFrame, colName: String): CountVectorizerModel = {
    val cvModel: CountVectorizerModel = new CountVectorizer()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")
      .setMinDF(2)
      .fit(df)

    //    for (i <- 0 until cvModel.vocabulary.size) {
    //      print(i + ":" + cvModel.vocabulary(i) + " ")
    //    }
    //    println()
    cvModel
  }

  def chiSqSelectorFpr(df: DataFrame, colName: String, target: String, para: Double): DataFrame = {
    val selector = new ChiSqSelector().setSelectorType("fpr")
      .setFpr(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName)

    val result: ChiSqSelectorModel = selector.fit(df)

    println(s"ChiSqSelector output with top ${result.selectedFeatures.size} ${colName} selected")

    result.transform(df)
  }

  def chiSqSelectorNum(cv: CountVectorizerModel, df: DataFrame, colName: String, target: String, para: Int): DataFrame = {
    val selector = new ChiSqSelector().setSelectorType("numTopFeatures")
      .setNumTopFeatures(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName)

    val result: ChiSqSelectorModel = selector.fit(cv.transform(df))

    println(s"ChiSqSelector output with top ${result.selectedFeatures.size} ${colName} selected")

    for (f <- result.selectedFeatures) {
      print(cv.vocabulary(f) + ",")
    }
    println()

    result.transform(cv.transform(df))
  }

  def chiSqSelectorPer(cv: CountVectorizerModel, df: DataFrame, colName: String, target: String, para: Double): DataFrame = {
    val selector = new ChiSqSelector().setSelectorType("percentile")
      .setPercentile(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName)

    val result: ChiSqSelectorModel = selector.fit(cv.transform(df))

    println(s"ChiSqSelector output with top ${result.selectedFeatures.size} ${colName} selected")

    for (f <- result.selectedFeatures) {
      print(cv.vocabulary(f) + ";")
    }
    println()

    result.transform(cv.transform(df))
  }

  def chiSqSelectorFpr(cv: CountVectorizerModel, df: DataFrame, colName: String, target: String, para: Double): DataFrame = {
    val selector = new ChiSqSelector().setSelectorType("fpr")
      .setFpr(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName)

    val result: ChiSqSelectorModel = selector.fit(cv.transform(df))

    println(s"ChiSqSelector output with top ${result.selectedFeatures.size} ${colName} selected")

    for (f <- result.selectedFeatures) {
      print(cv.vocabulary(f) + ";")
    }
    println()

    result.transform(cv.transform(df))
  }

  def extractAndSelectNum(df: DataFrame, colName: String, target: String, para: Int): DataFrame = {
    val cv = extractFeatures(df, colName)
    val result = chiSqSelectorNum(cv, df, colName + "_features", target, para)
    result
  }

  def extractAndSelectPer(df: DataFrame, colName: String, target: String, para: Double): DataFrame = {
    val cv = extractFeatures(df, colName)
    val result = chiSqSelectorPer(cv, df, colName + "_features", target, para)
    result
  }

  def extractAndSelectFpr(df: DataFrame, colName: String, target: String, para: Double): DataFrame = {
    val cv = extractFeatures(df, colName)
    val result = chiSqSelectorFpr(cv, df, colName + "_features", target, para)
    result
  }
}

case class PlatformETS() extends ColumnFeatureETS {
}

case class categoriesETS() extends ColumnFeatureETS {
}

case class tagsETS() extends ColumnFeatureETS {
}

case class developerETS() extends ColumnFeatureETS {
}

case class publisherETS() extends ColumnFeatureETS {
}

case class priceETS() extends ColumnFeatureETS {
  def minMaxSca(df: DataFrame, colName: String): DataFrame = {
    val scaler = new MinMaxScaler()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")

    val scalerModel = scaler.fit(df)
    scalerModel.transform(df)
  }

}

case class EtsHelper() extends ColumnFeatureETS {
  def vectorAss(df: DataFrame, colName: String): DataFrame = {
    val assembler = new VectorAssembler()
      .setInputCols(Array("selected_price_features", "selected_platforms_features", "selected_categories_features", "selected_tags_features",
        "selected_developer_features", "selected_publisher_features"))
      .setOutputCol("features")
    assembler.transform(df).drop("appid","developer","publisher","platforms","categories","tags","price",
      "appid_features","developer_features","publisher_features","platforms_features","categories_features","tags_features","price_features",
      "selected_appid_features","selected_developer_features","selected_publisher_features","selected_platforms_features",
      "selected_categories_features","selected_tags_features","selected_price_features"
    )
  }

}