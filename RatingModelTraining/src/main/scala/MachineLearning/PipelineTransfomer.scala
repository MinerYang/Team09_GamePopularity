package MachineLearning

import java.io.{File, PrintWriter}

import org.apache.spark.ml.feature.IndexToString

//import app.Stages_constructor._
import org.apache.spark.ml.feature.{ChiSqSelector, ChiSqSelectorModel, CountVectorizer, CountVectorizerModel, MinMaxScaler, StringIndexer, VectorAssembler}
import org.apache.spark.sql.DataFrame
object PipelineTransfomer {

  var target = "ratings"
  var threshold = 0.05
  //for new and init transformers
  def indexer :StringIndexer = labelIndex("ratings")
  def t0 :VectorAssembler = priceVector("price")
  def t1 :MinMaxScaler = minMaxScaler("price_vec")
  def t2 :ChiSqSelector = chiSqSelector("fpr", "price_vec_features", target, threshold)
  def t3 :CountVectorizer = countVectorize("platforms")
  def t4 :ChiSqSelector = chiSqSelector("fdr", "platforms_features", target, threshold)
  def t5 :CountVectorizer  = countVectorize("categories")
  def t6 :ChiSqSelector = chiSqSelector("fdr", "categories_features", target, threshold)
  def t7 :CountVectorizer = countVectorize("tags")
  def t8 :ChiSqSelector = chiSqSelector("fdr", "tags_features", target, threshold)
  def t9 :CountVectorizer = countVectorize("developer")
  def t10 :ChiSqSelector  = chiSqSelector("fdr", "developer_features", target, threshold)
  def t11 :CountVectorizer = countVectorize("publisher")
  def t12 :ChiSqSelector = chiSqSelector("fdr", "publisher_features", target, threshold)
  def fs  :VectorAssembler = featuresAssemb

  def labelIndex(colName:String) : StringIndexer={
    println(s"*** init a StringIndexer for $colName")
    new StringIndexer()
      .setInputCol(colName)
      .setOutputCol("label")
  }

  def priceVector(colName: String) : VectorAssembler = {
    println(s"*** init a VectorAssembler for $colName")
    val col = Array(
      colName
    )
    new VectorAssembler()
      .setInputCols(col)
      .setOutputCol("price_vec")
  }

  def minMaxScaler(colName: String): MinMaxScaler = {
    println("*** init a MinMaxScaler with column" + colName)
    new MinMaxScaler()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")
  }

  def chiSqSelector(stype: String, colName: String, target: String, para: Double): ChiSqSelector = {
    println("*** init a ChiSqSelector type:" + stype + " with column: " + colName)
    if (stype.equals("fpr")) {
      val selector = new ChiSqSelector()
        .setSelectorType("fpr")
        .setFpr(para)
        .setFeaturesCol(colName)
        .setLabelCol(target)
        .setOutputCol("selected_" + colName)
      return selector
    }
    else if (stype.equals("fdr")) {
      val selector = new ChiSqSelector()
        .setSelectorType("fdr")
        .setFdr(para)
        .setFeaturesCol(colName)
        .setLabelCol(target)
        .setOutputCol("selected_" + colName)
      return selector
    }
    println("no selector type defined")
    val selector = new ChiSqSelector()
    selector
  }

  def countVectorize(colName: String): CountVectorizer = {
    println("*** init a CountVectorizer with column: " + colName)
    new CountVectorizer()
      .setInputCol(colName)
      .setOutputCol(colName + "_features")
      .setMinDF(2)
  }

  def featuresAssemb: VectorAssembler = {
    println("*** init a VectorAssembler for features")
    val featureCol = Array(
      "selected_price_vec_features",
      "selected_platforms_features",
      "selected_categories_features",
      "selected_tags_features",
      "selected_developer_features",
      "selected_publisher_features"
    )
    new VectorAssembler()
      .setInputCols(featureCol)
      .setOutputCol("features")
  }


  /**
   * transform into usable dataFrame using CountVectorizer and ChiSqSelector
   * this is only for verifying and futher use
   * nothing to do with pipeline stages
   * @param cv
   * @param chiq
   * @param colname
   * @param df
   * @return
   */
  def frameTransform(cv:CountVectorizer, chiq:ChiSqSelector, colname:String, df:DataFrame,path:String): DataFrame ={
    val cvmodel :CountVectorizerModel = cv.fit(df)
    val df1 = cvmodel.transform(df)
    val chiqmodel: ChiSqSelectorModel = chiq.fit(df1)
    printselect(cvmodel,chiqmodel,colname,path)
    chiqmodel.transform(df1)
  }

  /**
   * print selected features to txt file for futher use
   * @param cvmodel
   * @param selmodel
   * @param colname
   */
  def printselect(cvmodel:CountVectorizerModel, selmodel:ChiSqSelectorModel, colname:String, path:String): Unit ={
    println(s"selected top ${selmodel.selectedFeatures.size} ${colname} as features")
    // Creating a file
    val file_Object = new File(s"${path}/selectionFile/select_${colname}.txt" )
    // Passing reference of file to the printwriter
    val print_Writer = new PrintWriter(file_Object)
    // Writing to the file
    for (f <- selmodel.selectedFeatures) {
      val str = cvmodel.vocabulary(f) + ";"
      print_Writer.write(str)
    }
    println(s"$colname selections write to local file select_$colname.txt\n")
    // Closing printwriter
    print_Writer.close()
  }

}
