package data

import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel, CountVectorizer, CountVectorizerModel, MinMaxScaler, MinMaxScalerModel}
import org.apache.spark.sql.DataFrame

object PL_transfomer {

  def minMaxScaler(df: DataFrame, colName: String): MinMaxScalerModel = {
    val scaler = new MinMaxScaler()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")
    scaler.fit(df)
  }


  def countVectorize(df:DataFrame, colName:String):CountVectorizerModel = {
    val cv = new CountVectorizer()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")
      .setMinDF(2)
    cv.fit(df)
  }


  def chiSqSelectorFpr(df: DataFrame, colName: String, target: String, para: Double): ChiSqSelectorModel ={
    val selector = new ChiSqSelector()
      .setSelectorType("fpr")
      .setFpr(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName)
    selector.fit(df)
  }


//  def chiSqSelectorNum(cv: CountVectorizerModel, df: DataFrame, colName: String, para: Int):ChiSqSelectorModel = {
//    val selector = new ChiSqSelector()
//      .setSelectorType("numTopFeatures")
//      .setNumTopFeatures(para)
//      .setFeaturesCol(colName)
//      .setLabelCol("ratings")
//      .setOutputCol("selected_" + colName)
//    selector.fit(cv.transform(df))
//  }
//
//  def chiSqSelectorPer(cv: CountVectorizerModel, df: DataFrame, colName: String, target: String, para: Double): ChiSqSelectorModel = {
//    val selector = new ChiSqSelector()
//      .setSelectorType("percentile")
//      .setPercentile(para)
//      .setFeaturesCol(colName)
//      .setLabelCol(target)
//      .setOutputCol("selected_" + colName)
//    selector.fit(cv.transform(df))
//  }

  def chiSqSelectorFdr(cv: CountVectorizerModel, df: DataFrame, colName: String, target: String, para: Double): ChiSqSelectorModel = {
    val selector = new ChiSqSelector()
      .setSelectorType("fdr")
      .setFdr(para)
      .setFeaturesCol(colName)
      .setLabelCol(target)
      .setOutputCol("selected_" + colName)
    selector.fit(cv.transform(df))
  }

  /**
   * chaining countVectorize and then chiSqSelectorFdr
   * @param df
   * @param colName
   * @param target
   * @param para
   * @return
   */
  def cvNfdr(df:DataFrame, colName:String, target:String, para:Double):ChiSqSelectorModel = {
    val cv = countVectorize(df, colName)
    chiSqSelectorFdr(cv,df,colName,target,para)
  }









}
