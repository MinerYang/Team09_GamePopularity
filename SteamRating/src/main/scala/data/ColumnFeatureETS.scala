package data

import org.apache.spark.ml.feature.{CountVectorizer, CountVectorizerModel, ChiSqSelector, ChiSqSelectorModel}
import org.apache.spark.sql.DataFrame

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

  def chiSqSelectorNum(cv: CountVectorizerModel, df: DataFrame, colName: String, target: String, para: Int): DataFrame = {
    val selector = new ChiSqSelector().setSelectorType("numTopFeatures")
      .setNumTopFeatures(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName + "_Features")

    val result: ChiSqSelectorModel = selector.fit(cv.transform(df))

    println(s"ChiSqSelector output with top ${result.selectedFeatures.size} ${colName} selected")

    for (f <- result.selectedFeatures) {
      print(cv.vocabulary(f) + " ")
    }
    println()

    result.transform(cv.transform(df))
  }

  def chiSqSelectorPer(cv: CountVectorizerModel, df: DataFrame, colName: String, target: String, para: Double): DataFrame = {
    val selector = new ChiSqSelector().setSelectorType("percentile")
      .setPercentile(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName + "_Features")

    val result: ChiSqSelectorModel = selector.fit(cv.transform(df))

    println(s"ChiSqSelector output with top ${result.selectedFeatures.size} ${colName} selected")

    for (f <- result.selectedFeatures) {
      print(cv.vocabulary(f) + " ")
    }
    println()

    result.transform(cv.transform(df))
  }

  def chiSqSelectorFpr(cv: CountVectorizerModel, df: DataFrame, colName: String, target: String, para: Double): DataFrame = {
    val selector = new ChiSqSelector().setSelectorType("fpr")
      .setFpr(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName + "_Features")

    val result: ChiSqSelectorModel = selector.fit(cv.transform(df))

    println(s"ChiSqSelector output with top ${result.selectedFeatures.size} ${colName} selected")

    for (f <- result.selectedFeatures) {
      print(cv.vocabulary(f) + " ")
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
}